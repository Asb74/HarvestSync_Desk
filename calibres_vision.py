"""Lógica de visión para detectar patrón circular y cálculo prudente de calibres."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    import cv2
    import numpy as np
except Exception:  # pragma: no cover - fallback explícito si OpenCV no está instalado
    cv2 = None
    np = None


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
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_foto": self.image_id,
            "patron_detectado": self.detected,
            "diametro_detectado_px": self.diameter_px,
            "mm_por_pixel": self.mm_per_pixel,
            "centro_x_px": self.center_x_px,
            "centro_y_px": self.center_y_px,
            "valida_para_siguiente_paso": self.valid_for_next_step,
            "error": self.error,
        }


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


class CirclePatternDetector:
    """Detector clásico (sin deep learning) basado en OpenCV."""

    def __init__(self, diametro_real_mm: float, max_detection_size: int = 1200) -> None:
        self.diametro_real_mm = float(diametro_real_mm)
        self.max_detection_size = max(int(max_detection_size), 400)

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

            circle = self._estimate_circle(frame)
            if circle is None:
                return CircleDetectionResult(
                    image_id=image_id,
                    detected=False,
                    diameter_px=None,
                    mm_per_pixel=None,
                    valid_for_next_step=False,
                    error="No se detectó patrón circular confiable.",
                )

            center_x, center_y, radius = circle
            diameter_px = radius * 2.0
            if diameter_px <= 0:
                return CircleDetectionResult(
                    image_id=image_id,
                    detected=False,
                    diameter_px=None,
                    mm_per_pixel=None,
                    valid_for_next_step=False,
                    error="Diámetro inválido detectado.",
                )

            mm_per_pixel = self.diametro_real_mm / diameter_px
            return CircleDetectionResult(
                image_id=image_id,
                detected=True,
                diameter_px=round(float(diameter_px), 2),
                mm_per_pixel=round(float(mm_per_pixel), 5),
                valid_for_next_step=True,
                center_x_px=round(float(center_x), 2),
                center_y_px=round(float(center_y), 2),
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
            if result.detected and result.center_x_px is not None and result.center_y_px is not None and result.diameter_px:
                cx = int(round(result.center_x_px))
                cy = int(round(result.center_y_px))
                radius = int(round(result.diameter_px / 2.0))
                cv2.circle(overlay, (cx, cy), max(radius, 1), color, 3)
                cv2.drawMarker(overlay, (cx, cy), (255, 255, 0), cv2.MARKER_CROSS, 28, 2)

            lines = [
                f"Foto: {result.image_id}",
                f"Patrón detectado: {'SI' if result.detected else 'NO'}",
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

    def _estimate_circle(self, frame: Any) -> tuple[float, float, float] | None:
        """Estima círculo en píxeles reales (cx, cy, r) usando imagen reducida para acelerar."""
        height, width = frame.shape[:2]
        longest = max(height, width)
        scale = 1.0
        if longest > self.max_detection_size:
            scale = self.max_detection_size / float(longest)
            frame = cv2.resize(frame, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

        circle_small = self._estimate_circle_on_frame(frame)
        if circle_small is None:
            return None

        cx, cy, radius = circle_small
        if scale != 1.0:
            inv = 1.0 / scale
            return (float(cx * inv), float(cy * inv), float(radius * inv))
        return (float(cx), float(cy), float(radius))

    def _estimate_circle_on_frame(self, frame: Any) -> tuple[float, float, float] | None:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (9, 9), 1.8)

        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(gray.shape[0], gray.shape[1]) * 0.25,
            param1=120,
            param2=30,
            minRadius=max(int(min(gray.shape[:2]) * 0.05), 8),
            maxRadius=max(int(min(gray.shape[:2]) * 0.48), 20),
        )

        if circles is not None and len(circles) > 0:
            candidates = circles[0]
            best = max(candidates, key=lambda c: c[2])
            return float(best[0]), float(best[1]), float(best[2])

        edges = cv2.Canny(gray, 60, 160)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        best_circle: tuple[float, float, float] | None = None
        best_area = 0.0

        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 250:
                continue
            perimeter = cv2.arcLength(contour, True)
            if perimeter <= 0:
                continue
            circularity = (4.0 * np.pi * area) / (perimeter * perimeter)
            if circularity < 0.75:
                continue
            (center_x, center_y), radius = cv2.minEnclosingCircle(contour)
            if area > best_area:
                best_area = area
                best_circle = (float(center_x), float(center_y), float(radius))

        return best_circle


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
            contours = self._segment_orange_contours(frame_scaled)
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
                    error="Sin candidatos tras segmentación HSV.",
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

    def _segment_orange_contours(self, frame: Any) -> list[Any]:
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

        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        return list(contours)

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
