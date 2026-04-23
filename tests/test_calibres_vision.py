from __future__ import annotations

import unittest

import cv2
import numpy as np

from calibres_vision import CirclePatternDetector


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


if __name__ == '__main__':
    unittest.main()
