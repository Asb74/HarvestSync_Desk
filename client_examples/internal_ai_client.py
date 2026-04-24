"""Cliente mínimo para HarvestSync Desk -> servicio interno IA.

Este cliente NO conoce ni maneja API keys de OpenAI.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from typing import Any


class InternalAIClientError(RuntimeError):
    pass


def call_analyze_image(
    *,
    server_url: str,
    image_path: str = "",
    image_url: str = "",
    task: str,
    context: str = "",
    cultivo: str = "",
    variedad: str = "",
    timeout_seconds: int = 20,
    internal_token: str = "",
) -> dict[str, Any]:
    body = {
        "task": task,
        "context": context,
    }
    if isinstance(cultivo, str) and cultivo.strip():
        body["cultivo"] = cultivo.strip()
    if isinstance(variedad, str) and variedad.strip():
        body["variedad"] = variedad.strip()
    if isinstance(image_url, str) and image_url.strip():
        body["image_url"] = image_url.strip()
    if isinstance(image_path, str) and image_path.strip():
        body["image_path"] = image_path.strip()

    headers = {"Content-Type": "application/json"}
    if internal_token:
        headers["X-Internal-Token"] = internal_token

    req = urllib.request.Request(
        url=f"{server_url.rstrip('/')}/analyze-image",
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            content = response.read().decode("utf-8")
            data = json.loads(content)
    except socket.timeout as exc:
        raise InternalAIClientError(f"Timeout de cliente agotado tras {timeout_seconds}s") from exc
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise InternalAIClientError(f"HTTP {exc.code} en servicio interno: {detail[:250]}") from exc
    except urllib.error.URLError as exc:
        raise InternalAIClientError(f"No se pudo conectar al servicio interno: {exc.reason}") from exc

    if not data.get("ok"):
        raise InternalAIClientError(f"Servicio interno devolvió error: {data}")

    return data["result"]


if __name__ == "__main__":
    # Ejemplo de uso desde HarvestSync Desk.
    result = call_analyze_image(
        server_url="http://SERVIDOR-HARVESTSYNC:8086",
        image_url="http://SERVIDOR-FOTOS/fotos/lote_123/box_123.jpg",
        task="validacion_foto",
        context="revisar si la caja está centrada y visible",
        timeout_seconds=20,
        # internal_token="TOKEN_INTERNO",  # activar si se configura autenticación simple
    )
    print(result)
