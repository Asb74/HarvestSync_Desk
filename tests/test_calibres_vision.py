from __future__ import annotations

import unittest

try:
    import cv2
    import numpy as np
except Exception:  # pragma: no cover
    cv2 = None
    np = None

from calibres_vision import CirclePatternDetector, FruitCaliberAnalyzer, medir_frutos_con_escala


@unittest.skipIf(cv2 is None or np is None, "OpenCV/numpy no disponibles en este entorno")
class TestCalibresVision(unittest.TestCase):
    def _encode_png(self, frame: np.ndarray) -> bytes:
        ok, buf = cv2.imencode('.png', frame)
        self.assertTrue(ok)
        return buf.tobytes()

    def test_detecta_patron_circular_y_calcula_escala(self) -> None:
        frame = np.zeros((500, 500, 3), dtype=np.uint8)
        cv2.circle(frame, (250, 250), 120, (255, 255, 255), thickness=8)
        raw = self._encode_png(frame)

        detector = CirclePatternDetector(diametro_real_mm=94.0)
        result = detector.detect_from_bytes('foto_1', raw)

        self.assertTrue(result.detected)
        self.assertTrue(result.valid_for_next_step)
        self.assertIn(result.detection_confidence, {"alta", "media", "baja"})
        self.assertIsNotNone(result.diameter_px)
        self.assertIsNotNone(result.mm_per_pixel)
        self.assertGreater(result.mm_per_pixel, 0.2)
        self.assertLess(result.mm_per_pixel, 1.0)

    def test_sin_patron_marca_invalida(self) -> None:
        frame = np.zeros((420, 420, 3), dtype=np.uint8)
        cv2.rectangle(frame, (120, 120), (320, 320), (255, 255, 255), thickness=-1)
        raw = self._encode_png(frame)

        detector = CirclePatternDetector(diametro_real_mm=94.0)
        result = detector.detect_from_bytes('foto_2', raw)

        self.assertFalse(result.detected)
        self.assertFalse(result.valid_for_next_step)
        self.assertIsNone(result.mm_per_pixel)

    def test_detecta_elipse_y_reporta_confianza(self) -> None:
        frame = np.zeros((640, 640, 3), dtype=np.uint8)
        cv2.ellipse(frame, (320, 320), (160, 120), 22, 0, 360, (255, 255, 255), thickness=10)
        raw = self._encode_png(frame)

        detector = CirclePatternDetector(diametro_real_mm=94.0)
        result = detector.detect_from_bytes("foto_elipse_1", raw)

        self.assertTrue(result.detected)
        self.assertTrue(result.valid_for_next_step)
        self.assertIsNotNone(result.diameter_px)
        self.assertIn(result.detection_confidence, {"alta", "media", "baja"})

    def test_analisis_frutos_descarta_parcial_y_clasifica_validos(self) -> None:
        frame = np.zeros((700, 700, 3), dtype=np.uint8)
        naranja = (0, 140, 255)
        cv2.circle(frame, (200, 260), 70, naranja, thickness=-1)   # válido
        cv2.circle(frame, (470, 260), 85, naranja, thickness=-1)   # válido
        cv2.circle(frame, (690, 520), 90, naranja, thickness=-1)   # toca borde => descarte
        raw = self._encode_png(frame)

        rangos = [
            {"nombre_calibre": "C1", "desde_mm": 55, "hasta_mm": 70},
            {"nombre_calibre": "C2", "desde_mm": 70, "hasta_mm": 95},
        ]
        analyzer = FruitCaliberAnalyzer()
        result = analyzer.analyze_photo("foto_frutos_1", raw, mm_per_pixel=0.5, caliber_ranges=rangos)

        self.assertTrue(result.photo_valid_for_phase)
        self.assertGreaterEqual(len(result.fruits), 3)
        validos = [f for f in result.fruits if f.valid]
        descartados = [f for f in result.fruits if not f.valid]
        self.assertGreaterEqual(len(validos), 2)
        self.assertGreaterEqual(len(descartados), 1)
        self.assertTrue(any((f.caliber_name or "").startswith("C") for f in validos))

    def test_analisis_frutos_sin_candidatos_invalida_foto(self) -> None:
        frame = np.zeros((500, 500, 3), dtype=np.uint8)
        raw = self._encode_png(frame)
        analyzer = FruitCaliberAnalyzer()
        result = analyzer.analyze_photo("foto_frutos_2", raw, mm_per_pixel=0.45, caliber_ranges=[])
        self.assertFalse(result.photo_valid_for_phase)
        self.assertEqual(len(result.fruits), 0)
        self.assertIsNotNone(result.error)

    def test_separa_fruta_agrupada_con_watershed(self) -> None:
        frame = np.zeros((900, 900, 3), dtype=np.uint8)
        naranja = (0, 140, 255)
        # Grupo muy junto (superposición importante)
        centers = [(330, 370), (430, 360), (520, 410), (400, 470), (500, 500)]
        for cx, cy in centers:
            cv2.circle(frame, (cx, cy), 92, naranja, thickness=-1)

        raw = self._encode_png(frame)
        analyzer = FruitCaliberAnalyzer()
        result = analyzer.analyze_photo("foto_frutos_watershed", raw, mm_per_pixel=0.45, caliber_ranges=[])

        # El caso previo tendía a detectar una sola masa. Ahora debe producir varios candidatos.
        self.assertGreaterEqual(len(result.fruits), 3)
        self.assertTrue(any(item.valid for item in result.fruits))

    def test_medir_frutos_con_escala_devuelve_mm_y_calibre(self) -> None:
        frame = np.zeros((720, 720, 3), dtype=np.uint8)
        naranja = (0, 140, 255)
        cv2.circle(frame, (220, 300), 80, naranja, thickness=-1)
        cv2.circle(frame, (470, 320), 78, naranja, thickness=-1)
        raw = self._encode_png(frame)

        rangos = [
            {"nombre_calibre": "CAL 0", "desde_mm": 85, "hasta_mm": 120},
            {"nombre_calibre": "CAL 3", "desde_mm": 65, "hasta_mm": 84.99},
            {"nombre_calibre": "CAL 9", "desde_mm": 0, "hasta_mm": 49.99},
        ]
        mediciones = medir_frutos_con_escala(raw, mm_por_px=0.6, rangos_calibres=rangos)
        self.assertGreaterEqual(len(mediciones), 2)
        self.assertTrue(all(item.diameter_mm > 70 for item in mediciones))
        self.assertTrue(all(item.calibre_estimado in {"CAL 0", "CAL 3"} for item in mediciones))


if __name__ == '__main__':
    unittest.main()
