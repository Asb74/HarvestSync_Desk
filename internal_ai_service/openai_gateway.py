"""Capa de acceso a OpenAI para mantener separada la lógica HTTP."""

from __future__ import annotations

import base64
import json
import logging
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


class OpenAIServiceError(RuntimeError):
    """Error controlado al hablar con OpenAI."""


class OpenAIGateway:
    def __init__(
        self,
        *,
        api_key_path: Path,
        model: str,
        timeout_seconds: int,
        base_url: str,
    ) -> None:
        self.api_key_path = api_key_path
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.base_url = base_url.rstrip("/")
        self.logger = logging.getLogger("harvestsync.internal_ai.gateway")

    def _read_api_key(self) -> str:
        if not self.api_key_path.exists():
            raise FileNotFoundError(f"API key file not found: {self.api_key_path}")

        api_key = self.api_key_path.read_text(encoding="utf-8").strip()
        if not api_key:
            raise ValueError("API key is empty")

        return api_key

    @staticmethod
    def _build_prompt(task: str, context: str) -> str:
        if task == "validacion_foto":
            return (
                "Eres un asistente experto en control de calidad agrícola para naranjas.\n"
                "Recibirás una foto de un box con frutas agrupadas.\n"
                "Objetivo: decidir si la imagen sirve para análisis muestral de calibres "
                "(estimación operativa), NO si es una foto perfecta.\n"
                "Criterio de aptitud:\n"
                "- NO marcar apta=false solo por oclusión media.\n"
                "- Marcar apta=false únicamente cuando la imagen no permite un análisis razonable, por ejemplo:\n"
                "  1) no se ve el patrón,\n"
                "  2) la foto está muy desenfocada,\n"
                "  3) el box no está suficientemente visible,\n"
                "  4) oclusión extrema,\n"
                "  5) muy pocas frutas visibles.\n"
                "Devuelve ÚNICAMENTE JSON válido, sin markdown ni texto extra, con este esquema exacto:\n"
                "{"
                "\"apta\": true/false, "
                "\"confianza\": 0-100, "
                "\"oclusion\": \"baja|media|alta\", "
                "\"patron_visible\": true/false, "
                "\"box_centrado\": true/false, "
                "\"frutas_visibles\": \"pocas|suficientes|muchas\", "
                "\"interferencia_patron\": \"baja|media|alta\", "
                "\"resumen\": \"texto breve\", "
                "\"alertas\": [\"...\"], "
                "\"recomendacion\": \"...\""
                "}\n"
                f"Tarea={task}. Contexto={context or 'sin contexto'}"
            )

        base_prompt = (
            "Eres un asistente para control de calidad agrícola. "
            "Devuelve un JSON compacto con campos: summary, alerts, confidence."
        )
        return f"{base_prompt} Tarea={task}. Contexto={context or 'sin contexto'}"

    def analyze_image(self, *, image_path: Path, task: str, context: str) -> dict[str, Any]:
        api_key = self._read_api_key()
        self.logger.info("gateway: inicio lectura imagen path=%s", image_path)
        image_bytes = image_path.read_bytes()
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        request_body = {
            "model": self.model,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": self._build_prompt(task, context)},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/jpeg;base64,{image_b64}",
                        },
                    ],
                }
            ],
        }

        payload = json.dumps(request_body).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}/responses",
            method="POST",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )

        try:
            self.logger.info("gateway: inicio llamada OpenAI model=%s timeout=%ss", self.model, self.timeout_seconds)
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw)
            self.logger.info("gateway: fin llamada OpenAI id=%s", parsed.get("id", ""))
        except socket.timeout as exc:
            raise TimeoutError("OpenAI request timeout") from exc
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise OpenAIServiceError(f"HTTP {exc.code}: {detail[:300]}") from exc
        except urllib.error.URLError as exc:
            raise OpenAIServiceError(f"OpenAI connection error: {exc.reason}") from exc

        return {
            "model": parsed.get("model", self.model),
            "output_text": parsed.get("output_text", ""),
            "raw_id": parsed.get("id", ""),
        }
