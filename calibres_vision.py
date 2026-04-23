"""Lógica de visión para detectar patrón circular y calcular escala mm/px."""
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
