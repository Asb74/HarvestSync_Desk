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
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_foto": self.image_id,
            "patron_detectado": self.detected,
            "diametro_detectado_px": self.diameter_px,
            "mm_por_pixel": self.mm_per_pixel,
            "valida_para_siguiente_paso": self.valid_for_next_step,
            "error": self.error,
        }


class CirclePatternDetector:
    """Detector clásico (sin deep learning) basado en OpenCV."""

    def __init__(self, diametro_real_mm: float) -> None:
        self.diametro_real_mm = float(diametro_real_mm)

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

            diameter_px = self._estimate_circle_diameter(frame)
            if diameter_px is None or diameter_px <= 0:
                return CircleDetectionResult(
                    image_id=image_id,
                    detected=False,
                    diameter_px=None,
                    mm_per_pixel=None,
                    valid_for_next_step=False,
                    error="No se detectó patrón circular confiable.",
                )

            mm_per_pixel = self.diametro_real_mm / diameter_px
            return CircleDetectionResult(
                image_id=image_id,
                detected=True,
                diameter_px=round(float(diameter_px), 2),
                mm_per_pixel=round(float(mm_per_pixel), 5),
                valid_for_next_step=True,
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

    def _estimate_circle_diameter(self, frame: Any) -> float | None:
        """Estima diámetro en píxeles con Hough + fallback por contornos."""
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
            return float(best[2] * 2.0)

        edges = cv2.Canny(gray, 60, 160)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        best_diameter = None
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
            (_, _), radius = cv2.minEnclosingCircle(contour)
            diameter = float(radius * 2.0)
            if area > best_area:
                best_area = area
                best_diameter = diameter

        return best_diameter
