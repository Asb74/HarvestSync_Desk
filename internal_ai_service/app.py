"""Servicio interno HarvestSync para proxy seguro a OpenAI.

- No expone la API key a clientes.
- Lee la clave únicamente desde servidor.
- Proporciona endpoints internos mínimos para salud y análisis de imagen.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from internal_ai_service.config import Settings, load_settings
from internal_ai_service.openai_gateway import OpenAIGateway, OpenAIServiceError

logger = logging.getLogger("harvestsync.internal_ai")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


app = Flask(__name__)
_config: Settings | None = None
_gateway: OpenAIGateway | None = None


def get_config() -> Settings:
    global _config
    if _config is None:
        _config = load_settings()
    return _config


def get_gateway() -> OpenAIGateway:
    global _gateway
    if _gateway is None:
        cfg = get_config()
        _gateway = OpenAIGateway(
            api_key_path=cfg.openai_key_path,
            model=cfg.openai_model,
            timeout_seconds=cfg.openai_timeout_seconds,
            base_url=cfg.openai_base_url,
        )
    return _gateway


def _forbidden() -> Any:
    return jsonify({"ok": False, "error": "forbidden"}), 403


@app.before_request
def check_internal_auth() -> Any:
    cfg = get_config()

    # Protección opcional con token compartido interno.
    if cfg.internal_token:
        header_value = request.headers.get("X-Internal-Token", "")
        if header_value != cfg.internal_token:
            logger.warning("Acceso denegado por token inválido desde %s", request.remote_addr)
            return _forbidden()

    # Lista blanca básica por IP (si está configurada).
    if cfg.allowed_client_ips:
        remote_ip = request.remote_addr or ""
        if remote_ip not in cfg.allowed_client_ips:
            logger.warning("IP fuera de whitelist: %s", remote_ip)
            return _forbidden()

    return None


@app.get("/health")
def health() -> Any:
    """Estado básico del servicio y presencia de clave."""
    cfg = get_config()
    key_file_exists = cfg.openai_key_path.exists()
    return jsonify(
        {
            "ok": True,
            "service": "harvestsync-internal-ai",
            "key_file_exists": key_file_exists,
            "model": cfg.openai_model,
        }
    )


@app.post("/analyze-image")
def analyze_image() -> Any:
    """Analiza una imagen del servidor mediante OpenAI.

    Body JSON esperado:
    {
      "image_path": "C:/ruta/a/imagen.jpg",
      "task": "validacion_foto",
      "context": "texto opcional"
    }
    """
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    image_path_value = payload.get("image_path")
    task = payload.get("task", "analisis_general")
    context = payload.get("context", "")

    if not isinstance(image_path_value, str) or not image_path_value.strip():
        return jsonify({"ok": False, "error": "invalid_image_path"}), 400

    image_path = Path(image_path_value)
    if not image_path.exists() or not image_path.is_file():
        return jsonify({"ok": False, "error": "image_not_found"}), 404

    if not isinstance(task, str) or len(task.strip()) == 0:
        return jsonify({"ok": False, "error": "invalid_task"}), 400

    if not isinstance(context, str):
        return jsonify({"ok": False, "error": "invalid_context"}), 400

    try:
        result = get_gateway().analyze_image(
            image_path=image_path,
            task=task.strip(),
            context=context.strip(),
        )
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "openai_key_file_missing"}), 500
    except ValueError as exc:
        # No exponemos detalles sensibles, pero sí diagnóstico útil.
        if "empty" in str(exc):
            return jsonify({"ok": False, "error": "openai_key_empty"}), 500
        return jsonify({"ok": False, "error": "invalid_config"}), 500
    except TimeoutError:
        return jsonify({"ok": False, "error": "openai_timeout"}), 504
    except OpenAIServiceError as exc:
        return jsonify({"ok": False, "error": "openai_error", "detail": str(exc)}), 502
    except Exception:  # pragma: no cover - fallback defensivo
        logger.error("Error no controlado en /analyze-image\n%s", traceback.format_exc())
        return jsonify({"ok": False, "error": "internal_error"}), 500

    return jsonify({"ok": True, "result": result})


if __name__ == "__main__":
    _configure_logging()
    cfg = get_config()
    app.run(host=cfg.bind_host, port=cfg.bind_port)
