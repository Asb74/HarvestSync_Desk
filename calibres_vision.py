"""Lógica de visión para detectar patrón circular y cálculo prudente de calibres."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

try:
    import cv2
    import numpy as np
except Exception:  # pragma: no cover - fallback explícito si OpenCV no está instalado
    cv2 = None
    np = None

LOGGER = logging.getLogger(__name__)


@dataclass
class CircleDetectionResult:
    """Resultado de detección para una imagen."""

    image_id: str
    detected: bool
    diameter_px: float | None
    mm_per_pixel: float | None
    valid_for_next_step: bool
    center_x_px: float | None = None
    center_y_px: float | None = None
    detection_confidence: str | None = None
    detection_method: str | None = None
    marker_contour: list[tuple[int, int]] | None = None
    inner_ellipse: tuple[float, float, float, float, float] | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_foto": self.image_id,
            "patron_detectado": self.detected,
            "diametro_detectado_px": self.diameter_px,
            "mm_por_pixel": self.mm_per_pixel,
            "centro_x_px": self.center_x_px,
            "centro_y_px": self.center_y_px,
            "confianza_deteccion": self.detection_confidence,
            "metodo_deteccion": self.detection_method,
            "valida_para_siguiente_paso": self.valid_for_next_step,
            "escala_fisica_fiable": self.valid_for_next_step,
            "error": self.error,
        }


@dataclass
class PatternCandidate:
    center_x: float
    center_y: float
    diameter_px: float
    score: float
    confidence: str
    method: str
    marker_contour: list[tuple[int, int]] | None = None
    inner_ellipse: tuple[float, float, float, float, float] | None = None


@dataclass
class FruitDetection:
    """Resultado individual por fruto detectado."""

    fruit_id: str
    contour: Any
    diameter_px: float | None
    diameter_mm: float | None
    caliber_name: str | None
    valid: bool
    discard_reason: str | None
    quality_score: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_fruto": self.fruit_id,
            "diametro_px": self.diameter_px,
            "diametro_mm": self.diameter_mm,
            "calibre": self.caliber_name,
            "valido": self.valid,
            "motivo_descarte": self.discard_reason,
            "confianza": self.quality_score,
        }


@dataclass
class PhotoFruitAnalysisResult:
    """Resultados de frutos para una foto concreta."""

    image_id: str
    photo_valid_for_phase: bool
    fruits: list[FruitDetection]
    caliber_count: dict[str, int]
    caliber_percentage: dict[str, float]
    discard_percentage: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_foto": self.image_id,
            "valida_para_fase_frutos": self.photo_valid_for_phase,
            "error": self.error,
            "frutos": [item.to_dict() for item in self.fruits],
            "conteo_por_calibre": dict(self.caliber_count),
            "porcentaje_por_calibre": dict(self.caliber_percentage),
            "porcentaje_descarte": self.discard_percentage,
        }


@dataclass
class PhotoFruitMeasurement:
    """Medición de diámetro por fruto usando escala física mm/px."""

    id: str
    center_x: float
    center_y: float
    diameter_px: float
    diameter_mm: float
    calibre_estimado: str
    confianza_medicion: str
    motivo: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "center_x": self.center_x,
            "center_y": self.center_y,
            "diameter_px": self.diameter_px,
            "diameter_mm": self.diameter_mm,
            "calibre_estimado": self.calibre_estimado,
            "confianza_medicion": self.confianza_medicion,
            "motivo": self.motivo,
        }


class CirclePatternDetector:
    """Detector clásico (sin deep learning) basado en OpenCV."""

    def __init__(self, diametro_real_mm: float, max_detection_size: int = 1200) -> None:
        self.diametro_real_mm = float(diametro_real_mm)
        self.max_detection_size = max(int(max_detection_size), 400)
        self.min_pattern_diameter_ratio = 0.02
        self.max_pattern_diameter_ratio = 0.35
        self.min_pattern_diameter_px = 40.0
        self.max_pattern_diameter_px = 900.0
        self.absurd_min_pattern_diameter_px = 40.0
        self.min_mm_per_px = 0.05
        self.max_mm_per_px = 1.5
        self.max_border_margin_ratio = 0.04

    def detect_from_bytes(self, image_id: str, raw_image: bytes) -> CircleDetectionResult:
        if cv2 is None or np is None:
            return CircleDetectionResult(
                image_id=image_id,
                detected=False,
                diameter_px=None,
                mm_per_pixel=None,
                valid_for_next_step=False,
                error="OpenCV/numpy no están disponibles en el entorno.",
            )

        if not raw_image:
            return CircleDetectionResult(
                image_id=image_id,
                detected=False,
                diameter_px=None,
                mm_per_pixel=None,
                valid_for_next_step=False,
                error="Imagen vacía o no descargada.",
            )

        try:
            image_array = np.frombuffer(raw_image, dtype=np.uint8)
            frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("No se pudo decodificar la imagen")

            candidate, reason = self._estimate_circle(frame)
            if candidate is None:
                return CircleDetectionResult(
                    image_id=image_id,
                    detected=False,
                    diameter_px=None,
                    mm_per_pixel=None,
                    valid_for_next_step=False,
                    error=reason or "no_se_detecto_patron_confiable",
                )

            diameter_px = float(candidate.diameter_px)
            if diameter_px <= 0:
                return CircleDetectionResult(
                    image_id=image_id,
                    detected=False,
                    diameter_px=None,
                    mm_per_pixel=None,
                    valid_for_next_step=False,
                    detection_confidence="baja",
                    error="diametro_invalido",
                )

            mm_per_pixel = self.diametro_real_mm / diameter_px
            escala_fiable = bool(self.min_mm_per_px <= mm_per_pixel <= self.max_mm_per_px)
            error = "ok" if escala_fiable else "escala_fuera_rango"
            if not escala_fiable and candidate.confidence != "baja":
                candidate.confidence = "baja"
            LOGGER.info(
                "Detección patrón: foto=%s diametro_px=%.2f mm_por_px=%.5f confianza=%s metodo=%s valida=%s",
                image_id,
                diameter_px,
                mm_per_pixel,
                candidate.confidence,
                candidate.method,
                escala_fiable,
            )
            return CircleDetectionResult(
                image_id=image_id,
                detected=True,
                diameter_px=round(float(diameter_px), 2),
                mm_per_pixel=round(float(mm_per_pixel), 5),
                valid_for_next_step=escala_fiable,
                center_x_px=round(float(candidate.center_x), 2),
                center_y_px=round(float(candidate.center_y), 2),
                detection_confidence=candidate.confidence,
                detection_method=candidate.method,
                marker_contour=candidate.marker_contour,
                inner_ellipse=candidate.inner_ellipse,
                error=error,
            )
        except Exception as exc:  # noqa: BLE001
            return CircleDetectionResult(
                image_id=image_id,
                detected=False,
                diameter_px=None,
                mm_per_pixel=None,
                valid_for_next_step=False,
                error=str(exc),
            )

    def build_overlay_bytes(self, raw_image: bytes, result: CircleDetectionResult) -> bytes | None:
        """Genera PNG anotado para validación visual de la detección."""
        if cv2 is None or np is None or not raw_image:
            return None

        try:
            image_array = np.frombuffer(raw_image, dtype=np.uint8)
            frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
            if frame is None:
                return None

            overlay = frame.copy()
            color = (0, 170, 0) if result.detected else (0, 0, 220)
            if result.marker_contour:
                cnt = np.array(result.marker_contour, dtype=np.int32).reshape((-1, 1, 2))
                cv2.polylines(overlay, [cnt], True, (255, 80, 80), 3)
            if result.inner_ellipse:
                ex, ey, ew, eh, angle = result.inner_ellipse
                cv2.ellipse(
                    overlay,
                    (int(round(ex)), int(round(ey))),
                    (max(int(round(ew / 2.0)), 1), max(int(round(eh / 2.0)), 1)),
                    float(angle),
                    0,
                    360,
                    (0, 255, 255),
                    2,
                )
            if result.detected and result.center_x_px is not None and result.center_y_px is not None and result.diameter_px:
                cx = int(round(result.center_x_px))
                cy = int(round(result.center_y_px))
                radius = int(round(result.diameter_px / 2.0))
                cv2.circle(overlay, (cx, cy), max(radius, 1), color, 3)
                cv2.drawMarker(overlay, (cx, cy), (255, 255, 0), cv2.MARKER_CROSS, 28, 2)

            lines = [
                f"Foto: {result.image_id}",
                f"Patrón detectado: {'SI' if result.detected else 'NO'}",
                f"Confianza: {result.detection_confidence or '-'}",
                f"Método: {result.detection_method or '-'}",
                f"Diametro px: {result.diameter_px if result.diameter_px is not None else '-'}",
                f"mm/px: {result.mm_per_pixel if result.mm_per_pixel is not None else '-'}",
                f"Valida: {'SI' if result.valid_for_next_step else 'NO'}",
            ]
            if result.error:
                lines.append(f"Error: {result.error}")

            y = 30
            for line in lines:
                cv2.putText(overlay, line, (16, y), cv2.FONT_HERSHEY_SIMPLEX, 0.72, (0, 0, 0), 4, cv2.LINE_AA)
                cv2.putText(overlay, line, (16, y), cv2.FONT_HERSHEY_SIMPLEX, 0.72, (255, 255, 255), 1, cv2.LINE_AA)
                y += 30

            ok, encoded = cv2.imencode(".png", overlay)
            if not ok:
                return None
            return encoded.tobytes()
        except Exception:
            return None

    def _estimate_circle(self, frame: Any) -> tuple[PatternCandidate | None, str | None]:
        """Estima patrón circular/elíptico en píxeles reales usando imagen reducida para acelerar."""
        height, width = frame.shape[:2]
        longest = max(height, width)
        scale = 1.0
        if longest > self.max_detection_size:
            scale = self.max_detection_size / float(longest)
            frame = cv2.resize(frame, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

        candidate_small, reason = self._estimate_circle_on_frame(frame)
        if candidate_small is None:
            return None, reason or "no_se_detecto_patron_confiable"

        if scale != 1.0:
            inv = 1.0 / scale
            return PatternCandidate(
                center_x=float(candidate_small.center_x * inv),
                center_y=float(candidate_small.center_y * inv),
                diameter_px=float(candidate_small.diameter_px * inv),
                score=float(candidate_small.score),
                confidence=candidate_small.confidence,
                method=candidate_small.method,
                marker_contour=[
                    (int(round(point[0] * inv)), int(round(point[1] * inv)))
                    for point in (candidate_small.marker_contour or [])
                ]
                or None,
                inner_ellipse=(
                    float(candidate_small.inner_ellipse[0] * inv),
                    float(candidate_small.inner_ellipse[1] * inv),
                    float(candidate_small.inner_ellipse[2] * inv),
                    float(candidate_small.inner_ellipse[3] * inv),
                    float(candidate_small.inner_ellipse[4]),
                )
                if candidate_small.inner_ellipse
                else None,
            ), None
        return candidate_small, None

    def _estimate_circle_on_frame(self, frame: Any) -> tuple[PatternCandidate | None, str | None]:
        gray, white_mask = self._prepare_marker_masks(frame)
        marker_candidates = self._find_marker_candidates(frame, white_mask)
        LOGGER.info("Detección patrón: candidatos marcador encontrados=%s", len(marker_candidates))

        reason_counts: dict[str, int] = {}
        best_candidate: PatternCandidate | None = None
        best_score = float("-inf")
        for marker in marker_candidates:
            marker_candidate, reason = self._analyze_marker_interior(frame, gray, marker)
            if reason is not None:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                continue
            if marker_candidate and marker_candidate.score > best_score:
                best_score = marker_candidate.score
                best_candidate = marker_candidate

        if best_candidate is not None:
            LOGGER.info(
                "Detección patrón: mejor candidato método=%s diametro_px=%.2f confianza=%s",
                best_candidate.method,
                best_candidate.diameter_px,
                best_candidate.confidence,
            )
            return best_candidate, None

        hough_candidate, hough_reason = self._detect_hough_global(frame, gray)
        if hough_candidate is not None:
            LOGGER.info(
                "Detección patrón: fallback global método=%s diametro_px=%.2f confianza=%s",
                hough_candidate.method,
                hough_candidate.diameter_px,
                hough_candidate.confidence,
            )
            return hough_candidate, None
        if hough_reason:
            reason_counts[hough_reason] = reason_counts.get(hough_reason, 0) + 1
        reason = self._select_failure_reason(reason_counts)
        return None, reason

    def _score_pattern_candidate(
        self,
        frame: Any,
        cx: float,
        cy: float,
        diameter_px: float,
        circularity: float,
        axis_ratio: float = 1.0,
    ) -> tuple[float, str | None, str]:
        height, width = frame.shape[:2]
        short_side = float(min(width, height))
        if diameter_px <= 0:
            return float("-inf"), "diametro_invalido", "baja"

        if diameter_px < self.absurd_min_pattern_diameter_px or diameter_px > (short_side * 0.95):
            return float("-inf"), "candidatos_descartados_por_tamano_absurdo", "baja"
        ratio = diameter_px / max(short_side, 1.0)
        cand_radius = diameter_px / 2.0

        margin = short_side * self.max_border_margin_ratio
        if (
            (cx - cand_radius) < margin
            or (cy - cand_radius) < margin
            or (cx + cand_radius) > (width - margin)
            or (cy + cand_radius) > (height - margin)
        ):
            return float("-inf"), "patron_cerca_borde", "baja"

        mm_per_pixel = self.diametro_real_mm / diameter_px
        scale_penalty = 0.0 if (self.min_mm_per_px <= mm_per_pixel <= self.max_mm_per_px) else 1.2

        center_bias_x = abs(cx - (width / 2.0)) / max(width / 2.0, 1.0)
        center_bias_y = abs(cy - (height / 2.0)) / max(height / 2.0, 1.0)
        center_penalty = center_bias_x + center_bias_y
        wide_range_penalty = 0.0
        if diameter_px < self.min_pattern_diameter_px:
            wide_range_penalty += min((self.min_pattern_diameter_px - diameter_px) / self.min_pattern_diameter_px, 1.0)
        elif diameter_px > self.max_pattern_diameter_px:
            wide_range_penalty += min((diameter_px - self.max_pattern_diameter_px) / self.max_pattern_diameter_px, 1.0)
        if ratio < self.min_pattern_diameter_ratio:
            wide_range_penalty += min((self.min_pattern_diameter_ratio - ratio) * 4.5, 1.0)
        elif ratio > self.max_pattern_diameter_ratio:
            wide_range_penalty += min((ratio - self.max_pattern_diameter_ratio) * 4.5, 1.0)

        contrast_score = self._contrast_score(frame, cx, cy, diameter_px / 2.0)
        circularity_score = max(0.0, min(circularity, 1.2))
        axis_score = max(0.0, min(axis_ratio, 1.0))
        score = (
            (5.5 * circularity_score)
            + (4.0 * contrast_score)
            + (2.2 * axis_score)
            - (2.4 * center_penalty)
            - (2.6 * wide_range_penalty)
            - (1.8 * scale_penalty)
        )
        confidence = "alta"
        if wide_range_penalty >= 0.9 or scale_penalty > 0:
            confidence = "media"
        if wide_range_penalty >= 1.4 or contrast_score < 0.20 or circularity_score < 0.62:
            confidence = "baja"
        return score, None, confidence

    def _prepare_marker_masks(self, frame: Any) -> tuple[Any, Any]:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (7, 7), 1.4)
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        white_hsv = cv2.inRange(hsv, (0, 0, 145), (180, 90, 255))
        white_adapt = cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            -6,
        )
        white_mask = cv2.bitwise_or(white_hsv, white_adapt)
        kernel = np.ones((5, 5), np.uint8)
        white_mask = cv2.morphologyEx(white_mask, cv2.MORPH_OPEN, kernel, iterations=1)
        white_mask = cv2.morphologyEx(white_mask, cv2.MORPH_CLOSE, kernel, iterations=2)
        return gray, white_mask

    def _find_marker_candidates(self, frame: Any, white_mask: Any) -> list[dict[str, Any]]:
        contours, _ = cv2.findContours(white_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        height, width = frame.shape[:2]
        area_img = float(height * width)
        candidates: list[dict[str, Any]] = []
        for contour in contours:
            area = float(cv2.contourArea(contour))
            if area < area_img * 0.001 or area > area_img * 0.55:
                LOGGER.debug("Marcador descartado por área: %.2f", area)
                continue
            x, y, w, h = cv2.boundingRect(contour)
            if w <= 0 or h <= 0:
                continue
            ratio = float(max(w, h) / max(min(w, h), 1))
            if ratio > 5.2:
                LOGGER.debug("Marcador descartado por proporción: %.2f", ratio)
                continue
            peri = cv2.arcLength(contour, True)
            approx = cv2.approxPolyDP(contour, 0.04 * peri, True) if peri > 0 else contour
            whiteness = float(np.mean(white_mask[y : y + h, x : x + w])) / 255.0
            candidates.append(
                {
                    "contour": contour,
                    "bbox": (x, y, w, h),
                    "approx_points": len(approx),
                    "whiteness": whiteness,
                    "area": area,
                }
            )
        candidates.sort(key=lambda item: (item["whiteness"], item["area"]), reverse=True)
        return candidates[:12]

    def _analyze_marker_interior(self, frame: Any, gray: Any, marker: dict[str, Any]) -> tuple[PatternCandidate | None, str | None]:
        x, y, w, h = marker["bbox"]
        pad = int(max(4, round(min(w, h) * 0.08)))
        x0, y0 = max(0, x - pad), max(0, y - pad)
        x1, y1 = min(frame.shape[1], x + w + pad), min(frame.shape[0], y + h + pad)
        roi_gray = gray[y0:y1, x0:x1]
        if roi_gray.size == 0:
            return None, "roi_marcador_vacia"

        roi_eq = cv2.equalizeHist(roi_gray)
        roi_blur = cv2.GaussianBlur(roi_eq, (7, 7), 1.3)
        inner_bin = cv2.adaptiveThreshold(roi_blur, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 29, 8)
        contours, _ = cv2.findContours(inner_bin, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)

        best_local: PatternCandidate | None = None
        best_score = float("-inf")
        for contour in contours:
            area = float(cv2.contourArea(contour))
            if area < max(40.0, (w * h) * 0.006):
                continue
            if len(contour) < 5:
                continue
            (center, axes, angle) = cv2.fitEllipse(contour)
            major = float(max(axes[0], axes[1]))
            minor = float(min(axes[0], axes[1]))
            if minor <= 0 or major <= 0:
                continue
            axis_ratio = minor / major
            if axis_ratio < 0.48:
                continue
            cx = float(center[0] + x0)
            cy = float(center[1] + y0)
            diameter_px = (major + minor) / 2.0
            circ = (4.0 * math.pi * area) / max(cv2.arcLength(contour, True) ** 2, 1.0)
            score, _, confidence = self._score_pattern_candidate(frame, cx, cy, diameter_px, circularity=circ, axis_ratio=axis_ratio)
            score += marker["whiteness"] * 2.5
            if score > best_score:
                best_score = score
                best_local = PatternCandidate(
                    center_x=cx,
                    center_y=cy,
                    diameter_px=float(diameter_px),
                    score=float(score),
                    confidence=confidence,
                    method="marcador_rectangular_elipse",
                    marker_contour=[(int(pt[0][0]), int(pt[0][1])) for pt in marker["contour"]],
                    inner_ellipse=(cx, cy, major, minor, float(angle)),
                )
        if best_local is not None:
            return best_local, None

        hough_in_roi = self._detect_hough_in_roi(frame, roi_blur, x0, y0, marker)
        if hough_in_roi is not None:
            return hough_in_roi, None
        return None, "sin_elipse_o_circulo_en_marcador"

    def _detect_hough_in_roi(self, frame: Any, roi_blur: Any, x0: int, y0: int, marker: dict[str, Any]) -> PatternCandidate | None:
        circles = cv2.HoughCircles(
            roi_blur,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(roi_blur.shape[0], roi_blur.shape[1]) * 0.2,
            param1=110,
            param2=24,
            minRadius=max(int(min(roi_blur.shape[:2]) * 0.08), 6),
            maxRadius=max(int(min(roi_blur.shape[:2]) * 0.46), 12),
        )
        if circles is None or len(circles) == 0:
            return None
        best: PatternCandidate | None = None
        best_score = float("-inf")
        for c in circles[0]:
            cx = float(c[0] + x0)
            cy = float(c[1] + y0)
            diameter_px = float(c[2] * 2.0)
            score, _, confidence = self._score_pattern_candidate(frame, cx, cy, diameter_px, circularity=1.0, axis_ratio=1.0)
            score += marker["whiteness"] * 1.8
            if score > best_score:
                best_score = score
                best = PatternCandidate(
                    center_x=cx,
                    center_y=cy,
                    diameter_px=diameter_px,
                    score=score,
                    confidence=confidence,
                    method="hough_circle",
                    marker_contour=[(int(pt[0][0]), int(pt[0][1])) for pt in marker["contour"]],
                )
        return best

    def _detect_hough_global(self, frame: Any, gray: Any) -> tuple[PatternCandidate | None, str | None]:
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.25,
            minDist=max(gray.shape[0], gray.shape[1]) * 0.28,
            param1=120,
            param2=32,
            minRadius=max(int(min(gray.shape[:2]) * 0.03), 8),
            maxRadius=max(int(min(gray.shape[:2]) * 0.38), 15),
        )
        if circles is None or len(circles) == 0:
            return None, "sin_candidatos_hough"
        best: PatternCandidate | None = None
        best_score = float("-inf")
        for candidate in circles[0]:
            cx, cy, radius = float(candidate[0]), float(candidate[1]), float(candidate[2])
            score, _, confidence = self._score_pattern_candidate(frame, cx, cy, radius * 2.0, circularity=1.0, axis_ratio=1.0)
            score -= 0.25
            if score > best_score:
                best_score = score
                best = PatternCandidate(
                    center_x=cx,
                    center_y=cy,
                    diameter_px=radius * 2.0,
                    score=score,
                    confidence=confidence,
                    method="fallback",
                )
        return best, None if best is not None else "hough_global_descartado"

    def _contrast_score(self, frame: Any, cx: float, cy: float, radius: float) -> float:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        height, width = gray.shape[:2]
        yy, xx = np.ogrid[:height, :width]
        dist = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)
        inner = dist <= max(radius * 0.88, 1.0)
        ring = (dist >= (radius * 0.88)) & (dist <= (radius * 1.18))
        outer = (dist > (radius * 1.18)) & (dist <= (radius * 1.46))
        if not np.any(inner) or not np.any(ring):
            return 0.0

        inner_mean = float(np.mean(gray[inner]))
        ring_mean = float(np.mean(gray[ring]))
        outer_mean = float(np.mean(gray[outer])) if np.any(outer) else inner_mean
        edge_contrast = abs(ring_mean - inner_mean) / 255.0
        bg_contrast = abs(ring_mean - outer_mean) / 255.0
        return max(0.0, min((edge_contrast * 0.65) + (bg_contrast * 0.35), 1.0))

    def _select_failure_reason(self, reason_counts: dict[str, int]) -> str:
        if reason_counts:
            top_reason = max(reason_counts.items(), key=lambda item: item[1])[0]
            LOGGER.info("Detección patrón: candidatos descartados=%s", reason_counts)
            return top_reason
        return "no_se_detecto_patron_confiable"


class FruitCaliberAnalyzer:
    """Detector conservador de frutos con estimación prudente de diámetro ecuatorial."""

    def __init__(self, max_detection_size: int = 1400) -> None:
        self.max_detection_size = max(int(max_detection_size), 500)

    def analyze_photo(
        self,
        image_id: str,
        raw_image: bytes,
        mm_per_pixel: float,
        caliber_ranges: list[dict[str, Any]],
    ) -> PhotoFruitAnalysisResult:
        if cv2 is None or np is None:
            return PhotoFruitAnalysisResult(
                image_id=image_id,
                photo_valid_for_phase=False,
                fruits=[],
                caliber_count={},
                caliber_percentage={},
                discard_percentage=100.0,
                error="OpenCV/numpy no están disponibles en el entorno.",
            )

        if not raw_image:
            return PhotoFruitAnalysisResult(
                image_id=image_id,
                photo_valid_for_phase=False,
                fruits=[],
                caliber_count={},
                caliber_percentage={},
                discard_percentage=100.0,
                error="Imagen vacía o no descargada.",
            )

        if mm_per_pixel <= 0:
            return PhotoFruitAnalysisResult(
                image_id=image_id,
                photo_valid_for_phase=False,
                fruits=[],
                caliber_count={},
                caliber_percentage={},
                discard_percentage=100.0,
                error="Escala mm/px inválida para la foto.",
            )

        try:
            image_array = np.frombuffer(raw_image, dtype=np.uint8)
            frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
            if frame is None:
                raise ValueError("No se pudo decodificar la imagen")

            frame_scaled, ratio = self._resize_if_needed(frame)
            contours = self._detect_fruit_candidates(frame_scaled)
            fruits: list[FruitDetection] = []

            image_h, image_w = frame_scaled.shape[:2]
            image_area = float(image_h * image_w)
            min_area = max(image_area * 0.00012, 180.0)
            max_area = image_area * 0.22
            min_diameter = max(min(image_h, image_w) * 0.018, 14.0)
            max_diameter = min(image_h, image_w) * 0.42

            for index, contour in enumerate(contours, start=1):
                area = float(cv2.contourArea(contour))
                perimeter = float(cv2.arcLength(contour, True))
                x, y, w, h = cv2.boundingRect(contour)
                touches_border = x <= 1 or y <= 1 or (x + w) >= (image_w - 1) or (y + h) >= (image_h - 1)

                reason: str | None = None
                if area < min_area:
                    reason = "Área insuficiente"
                elif area > max_area:
                    reason = "Área excesiva/fusión"
                elif touches_border:
                    reason = "Toca borde de imagen"

                circularity = 0.0
                if reason is None:
                    if perimeter <= 0:
                        reason = "Perímetro inválido"
                    else:
                        circularity = float((4.0 * np.pi * area) / (perimeter * perimeter))
                        if circularity < 0.68:
                            reason = "Circularidad baja"

                aspect_ratio = float(w / max(h, 1))
                if reason is None and not (0.72 <= aspect_ratio <= 1.35):
                    reason = "Relación de aspecto no compatible"

                hull = cv2.convexHull(contour)
                hull_area = float(cv2.contourArea(hull)) if hull is not None else 0.0
                solidity = area / hull_area if hull_area > 1 else 0.0
                if reason is None and solidity < 0.9:
                    reason = "Contorno irregular/fusionado"

                (cx, cy), radius = cv2.minEnclosingCircle(contour)
                encl_diameter = float(radius * 2.0)
                fill_ratio = area / max(np.pi * radius * radius, 1.0)
                if reason is None and fill_ratio < 0.62:
                    reason = "Contorno incompleto"

                eq_diameter = float(np.sqrt(4.0 * area / np.pi))
                diameter_px = min(eq_diameter, encl_diameter * 0.98)
                if reason is None and (diameter_px < min_diameter or diameter_px > max_diameter):
                    reason = "Tamaño fuera de rango esperado"

                scaled_diameter_px = diameter_px / ratio
                diameter_mm = scaled_diameter_px * mm_per_pixel
                caliber_name = self._assign_caliber(diameter_mm, caliber_ranges) if reason is None else None
                quality = self._quality_score(circularity, aspect_ratio, fill_ratio, solidity) if reason is None else None

                fruits.append(
                    FruitDetection(
                        fruit_id=f"fruto_{index:03d}",
                        contour=contour,
                        diameter_px=round(float(scaled_diameter_px), 2),
                        diameter_mm=round(float(diameter_mm), 2),
                        caliber_name=caliber_name,
                        valid=reason is None,
                        discard_reason=reason,
                        quality_score=quality,
                    )
                )

            valid_count = len([f for f in fruits if f.valid])
            discard_count = len(fruits) - valid_count

            if not fruits:
                return PhotoFruitAnalysisResult(
                    image_id=image_id,
                    photo_valid_for_phase=False,
                    fruits=[],
                    caliber_count={},
                    caliber_percentage={},
                    discard_percentage=100.0,
                    error="Sin candidatos tras segmentación/separación local.",
                )

            if valid_count == 0:
                return PhotoFruitAnalysisResult(
                    image_id=image_id,
                    photo_valid_for_phase=False,
                    fruits=fruits,
                    caliber_count={},
                    caliber_percentage={},
                    discard_percentage=100.0,
                    error="Todos los frutos candidatos fueron descartados por calidad geométrica.",
                )

            caliber_count: dict[str, int] = {}
            for fruit in fruits:
                if not fruit.valid:
                    continue
                key = fruit.caliber_name or "SIN_RANGO"
                caliber_count[key] = caliber_count.get(key, 0) + 1

            caliber_percentage = {
                key: round((count / valid_count) * 100.0, 2)
                for key, count in sorted(caliber_count.items(), key=lambda item: item[0])
            }
            discard_percentage = round((discard_count / max(len(fruits), 1)) * 100.0, 2)

            return PhotoFruitAnalysisResult(
                image_id=image_id,
                photo_valid_for_phase=True,
                fruits=fruits,
                caliber_count=caliber_count,
                caliber_percentage=caliber_percentage,
                discard_percentage=discard_percentage,
            )
        except Exception as exc:  # noqa: BLE001
            return PhotoFruitAnalysisResult(
                image_id=image_id,
                photo_valid_for_phase=False,
                fruits=[],
                caliber_count={},
                caliber_percentage={},
                discard_percentage=100.0,
                error=str(exc),
            )

    def build_overlay_bytes(self, raw_image: bytes, result: PhotoFruitAnalysisResult) -> bytes | None:
        if cv2 is None or np is None or not raw_image:
            return None

        try:
            image_array = np.frombuffer(raw_image, dtype=np.uint8)
            frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
            if frame is None:
                return None
            frame_scaled, ratio = self._resize_if_needed(frame)
            overlay = frame_scaled.copy()

            for fruit in result.fruits:
                contour = fruit.contour
                if contour is None:
                    continue
                color = (0, 180, 0) if fruit.valid else (20, 20, 220)
                cv2.drawContours(overlay, [contour], -1, color, 2)
                x, y, _, _ = cv2.boundingRect(contour)
                if fruit.valid:
                    txt = f"{fruit.diameter_mm:.1f}mm {fruit.caliber_name or '-'}"
                else:
                    txt = fruit.discard_reason or "descartado"
                cv2.putText(overlay, txt[:42], (x, max(y - 6, 16)), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 0, 0), 3, cv2.LINE_AA)
                cv2.putText(overlay, txt[:42], (x, max(y - 6, 16)), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (255, 255, 255), 1, cv2.LINE_AA)

            total = len(result.fruits)
            valid = len([item for item in result.fruits if item.valid])
            lines = [
                f"Foto: {result.image_id}",
                f"Detectados: {total}",
                f"Validos: {valid}",
                f"Descartados: {max(total - valid, 0)} ({result.discard_percentage:.1f}%)",
            ]
            if result.error:
                lines.append(f"Estado: {result.error}")

            y = 28
            for line in lines:
                cv2.putText(overlay, line, (12, y), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (0, 0, 0), 4, cv2.LINE_AA)
                cv2.putText(overlay, line, (12, y), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (255, 255, 255), 1, cv2.LINE_AA)
                y += 28

            if ratio != 1.0:
                overlay = cv2.resize(overlay, (frame.shape[1], frame.shape[0]), interpolation=cv2.INTER_LINEAR)

            ok, encoded = cv2.imencode('.png', overlay)
            if not ok:
                return None
            return encoded.tobytes()
        except Exception:
            return None

    def _resize_if_needed(self, frame: Any) -> tuple[Any, float]:
        h, w = frame.shape[:2]
        longest = max(h, w)
        if longest <= self.max_detection_size:
            return frame, 1.0
        ratio = self.max_detection_size / float(longest)
        resized = cv2.resize(frame, None, fx=ratio, fy=ratio, interpolation=cv2.INTER_AREA)
        return resized, ratio

    def _detect_fruit_candidates(self, frame: Any) -> list[Any]:
        """Obtiene candidatos individuales con enfoque local para fruta en contacto."""
        mask = self._build_orange_mask(frame)
        if cv2.countNonZero(mask) == 0:
            return []

        contours = self._split_touching_regions_with_watershed(frame, mask)
        if contours:
            return contours

        # Fallback conservador: componentes conectados directos si watershed no separa nada.
        contours_cc, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        return list(contours_cc)

    def _build_orange_mask(self, frame: Any) -> Any:
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        lower_1 = np.array([3, 70, 45], dtype=np.uint8)
        upper_1 = np.array([24, 255, 255], dtype=np.uint8)
        lower_2 = np.array([0, 70, 35], dtype=np.uint8)
        upper_2 = np.array([2, 255, 255], dtype=np.uint8)
        mask = cv2.inRange(hsv, lower_1, upper_1)
        mask2 = cv2.inRange(hsv, lower_2, upper_2)
        mask = cv2.bitwise_or(mask, mask2)

        kernel_open = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        kernel_close = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (9, 9))
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel_open, iterations=1)
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel_close, iterations=2)

        # Limpieza por componente para evitar ruido de color naranja pequeño.
        num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
        min_component_area = max(int(frame.shape[0] * frame.shape[1] * 0.00008), 80)
        cleaned = np.zeros_like(mask)
        for idx in range(1, num_labels):
            area = int(stats[idx, cv2.CC_STAT_AREA])
            if area >= min_component_area:
                cleaned[labels == idx] = 255

        return cleaned

    def _split_touching_regions_with_watershed(self, frame: Any, mask: Any) -> list[Any]:
        """Separa masas conectadas usando transformada de distancia + watershed."""
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        sure_bg = cv2.dilate(mask, kernel, iterations=2)

        dist = cv2.distanceTransform(mask, cv2.DIST_L2, 5)
        dist_max = float(dist.max())
        if dist_max <= 0.0:
            return []

        # Pico alto => centro de fruta. Umbral conservador para evitar sobre-segmentación.
        _, sure_fg = cv2.threshold(dist, dist_max * 0.34, 255, cv2.THRESH_BINARY)
        sure_fg = sure_fg.astype(np.uint8)
        sure_fg = cv2.morphologyEx(sure_fg, cv2.MORPH_OPEN, kernel, iterations=1)

        num_markers, markers = cv2.connectedComponents(sure_fg)
        if num_markers <= 1:
            return []

        markers = markers + 1
        unknown = cv2.subtract(sure_bg, sure_fg)
        markers[unknown == 255] = 0

        # Watershed opera in-place sobre copia.
        ws_input = frame.copy()
        markers = cv2.watershed(ws_input, markers)

        image_h, image_w = frame.shape[:2]
        border_margin = max(int(min(image_h, image_w) * 0.015), 4)
        contours: list[Any] = []
        for label in range(2, int(markers.max()) + 1):
            region_mask = np.uint8(markers == label) * 255
            if cv2.countNonZero(region_mask) < 80:
                continue
            cs, _ = cv2.findContours(region_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            if not cs:
                continue
            contour = max(cs, key=cv2.contourArea)
            x, y, w, h = cv2.boundingRect(contour)
            if x <= border_margin or y <= border_margin or (x + w) >= (image_w - border_margin) or (y + h) >= (image_h - border_margin):
                # Borde de box/imagen: visibilidad parcial, no fiable para diámetro ecuatorial.
                continue
            contours.append(contour)

        return contours

    def _assign_caliber(self, diameter_mm: float, caliber_ranges: list[dict[str, Any]]) -> str:
        if not caliber_ranges:
            return "SIN_RANGO"
        for row in caliber_ranges:
            start_mm = float(row.get("desde_mm", 0.0) or 0.0)
            end_mm = float(row.get("hasta_mm", 0.0) or 0.0)
            if start_mm <= diameter_mm <= end_mm:
                return str(row.get("nombre_calibre", "SIN_RANGO") or "SIN_RANGO")
        return "FUERA_RANGO"

    def _quality_score(self, circularity: float, aspect_ratio: float, fill_ratio: float, solidity: float) -> float:
        circ_score = max(min((circularity - 0.68) / 0.30, 1.0), 0.0)
        aspect_delta = abs(1.0 - aspect_ratio)
        aspect_score = max(min(1.0 - (aspect_delta / 0.35), 1.0), 0.0)
        fill_score = max(min((fill_ratio - 0.62) / 0.38, 1.0), 0.0)
        solid_score = max(min((solidity - 0.90) / 0.10, 1.0), 0.0)
        score = (circ_score * 0.4) + (aspect_score * 0.2) + (fill_score * 0.2) + (solid_score * 0.2)
        return round(float(score), 2)


def medir_frutos_con_escala(
    image_bytes: bytes,
    mm_por_px: float,
    rangos_calibres: list[dict[str, Any]],
) -> list[PhotoFruitMeasurement]:
    """Mide frutos completos/casi completos y estima calibre usando escala física."""
    if cv2 is None or np is None or not image_bytes or mm_por_px <= 0:
        return []

    analyzer = FruitCaliberAnalyzer()
    try:
        image_array = np.frombuffer(image_bytes, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        if frame is None:
            return []

        frame_scaled, ratio = analyzer._resize_if_needed(frame)
        contours = analyzer._detect_fruit_candidates(frame_scaled)
        if not contours:
            return []

        image_h, image_w = frame_scaled.shape[:2]
        image_area = float(image_h * image_w)
        min_area = max(image_area * 0.00012, 180.0)
        max_area = image_area * 0.22
        min_diameter = max(min(image_h, image_w) * 0.018, 14.0)
        max_diameter = min(image_h, image_w) * 0.42
        measurements: list[PhotoFruitMeasurement] = []

        for index, contour in enumerate(contours, start=1):
            area = float(cv2.contourArea(contour))
            perimeter = float(cv2.arcLength(contour, True))
            x, y, w, h = cv2.boundingRect(contour)
            touches_border = x <= 1 or y <= 1 or (x + w) >= (image_w - 1) or (y + h) >= (image_h - 1)

            if area < min_area or area > max_area or touches_border or perimeter <= 0:
                continue

            circularity = float((4.0 * np.pi * area) / (perimeter * perimeter))
            aspect_ratio = float(w / max(h, 1))
            if circularity < 0.68 or not (0.72 <= aspect_ratio <= 1.35):
                continue

            hull = cv2.convexHull(contour)
            hull_area = float(cv2.contourArea(hull)) if hull is not None else 0.0
            solidity = area / hull_area if hull_area > 1 else 0.0
            if solidity < 0.9:
                continue

            (cx, cy), radius = cv2.minEnclosingCircle(contour)
            encl_diameter = float(radius * 2.0)
            fill_ratio = area / max(np.pi * radius * radius, 1.0)
            if fill_ratio < 0.62:
                continue

            eq_diameter = float(np.sqrt(4.0 * area / np.pi))
            diameter_px_scaled = min(eq_diameter, encl_diameter * 0.98)
            if diameter_px_scaled < min_diameter or diameter_px_scaled > max_diameter:
                continue

            diameter_px = diameter_px_scaled / ratio
            diameter_mm = diameter_px * mm_por_px
            calibre = analyzer._assign_caliber(diameter_mm, rangos_calibres)
            quality = analyzer._quality_score(circularity, aspect_ratio, fill_ratio, solidity)

            confianza = "baja"
            motivo = "medición con oclusión/parcialidad probable"
            if quality >= 0.78:
                confianza = "alta"
                motivo = "fruto completo/casi completo con geometría consistente"
            elif quality >= 0.58:
                confianza = "media"
                motivo = "fruto usable con leve oclusión o deformación"

            measurements.append(
                PhotoFruitMeasurement(
                    id=f"fruto_{index:03d}",
                    center_x=round(float(cx / ratio), 2),
                    center_y=round(float(cy / ratio), 2),
                    diameter_px=round(float(diameter_px), 2),
                    diameter_mm=round(float(diameter_mm), 2),
                    calibre_estimado=calibre,
                    confianza_medicion=confianza,
                    motivo=motivo,
                )
            )
        return measurements
    except Exception:
        return []
