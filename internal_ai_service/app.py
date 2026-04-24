"""Servicio interno HarvestSync para proxy seguro a OpenAI.

- No expone la API key a clientes.
- Lee la clave únicamente desde servidor.
- Proporciona endpoints internos mínimos para salud y análisis de imagen.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import tempfile
import time
import traceback
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import urllib.error
import urllib.request

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


def _extract_cultivo_variedad_from_context(context: str) -> tuple[str, str]:
    if not isinstance(context, str) or not context.strip():
        return "", ""
    try:
        parsed = json.loads(context)
    except json.JSONDecodeError:
        return "", ""
    if not isinstance(parsed, dict):
        return "", ""
    cultivo = str(parsed.get("cultivo", "") or "").strip()
    variedad = str(parsed.get("variedad", "") or "").strip()
    return cultivo, variedad


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
    gateway = get_gateway()
    try:
        gateway.ensure_prompt_schema()
        gateway.seed_prompts_if_empty()
    except Exception as exc:  # pragma: no cover - diagnóstico
        logger.warning("health: no se pudo inicializar esquema prompts sqlite: %s", exc)
    prompt_health = gateway.get_prompt_db_health()
    return jsonify(
        {
            "ok": True,
            "service": "harvestsync-internal-ai",
            "key_file_exists": key_file_exists,
            "model": cfg.openai_model,
            **prompt_health,
        }
    )


@app.post("/analyze-image")
def analyze_image() -> Any:
    """Analiza una imagen del servidor mediante OpenAI.

    Body JSON esperado:
    {
      "image_url": "http://servidor-fotos/fotos/lote/foto.jpg",  # recomendado
      "image_path": "C:/ruta/a/imagen.jpg",  # compatibilidad opcional
      "task": "validacion_foto",
      "context": "texto opcional",
      "cultivo": "CITRICOS",
      "variedad": "VALENCIA DELTA"
    }
    """
    request_id = uuid.uuid4().hex[:10]
    remote_ip = request.remote_addr or "-"
    start_ts = time.perf_counter()
    logger.info("analyze_image: request recibido request_id=%s ip=%s", request_id, remote_ip)

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        logger.warning("analyze_image: invalid_json request_id=%s", request_id)
        return jsonify({"ok": False, "error": "invalid_json", "request_id": request_id}), 400

    image_url_value = payload.get("image_url")
    image_path_value = payload.get("image_path")
    task = payload.get("task", "analisis_general")
    context = payload.get("context", "")
    cultivo = str(payload.get("cultivo", "") or "").strip()
    variedad = str(payload.get("variedad", "") or "").strip()
    if not cultivo or not variedad:
        cultivo_ctx, variedad_ctx = _extract_cultivo_variedad_from_context(context)
        if not cultivo:
            cultivo = cultivo_ctx
        if not variedad:
            variedad = variedad_ctx
    logger.info(
        "analyze_image: payload parseado request_id=%s image_url=%r image_path=%r task=%r cultivo=%r variedad=%r context_len=%s",
        request_id,
        image_url_value,
        image_path_value,
        task,
        cultivo,
        variedad,
        len(context) if isinstance(context, str) else "invalid",
    )

    image_path: Path | None = None
    temporary_file_path: Path | None = None

    if isinstance(image_url_value, str) and image_url_value.strip():
        image_url = image_url_value.strip()
        parsed = urlparse(image_url)
        if parsed.scheme not in {"http", "https"}:
            logger.warning("analyze_image: invalid_image_url_scheme request_id=%s image_url=%s", request_id, image_url)
            return jsonify({"ok": False, "error": "invalid_image_url", "request_id": request_id}), 400

        timeout_download = 10
        logger.info(
            "analyze_image: inicio descarga image_url request_id=%s timeout=%ss image_url=%s",
            request_id,
            timeout_download,
            image_url,
        )
        try:
            with urllib.request.urlopen(image_url, timeout=timeout_download) as response:
                image_bytes = response.read()
        except socket.timeout:
            logger.error("analyze_image: image_download_timeout request_id=%s image_url=%s", request_id, image_url)
            return jsonify({"ok": False, "error": "image_download_timeout", "request_id": request_id}), 504
        except urllib.error.HTTPError as exc:
            logger.error(
                "analyze_image: image_url_http_error request_id=%s code=%s image_url=%s",
                request_id,
                exc.code,
                image_url,
            )
            return jsonify(
                {"ok": False, "error": "image_url_http_error", "request_id": request_id, "status_code": exc.code},
            ), 502
        except urllib.error.URLError as exc:
            logger.error("analyze_image: image_download_failed request_id=%s image_url=%s detail=%s", request_id, image_url, exc)
            return jsonify({"ok": False, "error": "image_download_failed", "request_id": request_id}), 502

        if not image_bytes:
            logger.error("analyze_image: empty_image_content request_id=%s image_url=%s", request_id, image_url)
            return jsonify({"ok": False, "error": "empty_image_content", "request_id": request_id}), 502

        suffix = Path(parsed.path).suffix or ".jpg"
        tmp = tempfile.NamedTemporaryFile(prefix="harvestsync-ai-", suffix=suffix, delete=False)
        tmp.write(image_bytes)
        tmp.flush()
        tmp.close()
        temporary_file_path = Path(tmp.name)
        image_path = temporary_file_path
        logger.info("analyze_image: descarga completada request_id=%s bytes=%s path=%s", request_id, len(image_bytes), image_path)
    elif isinstance(image_path_value, str) and image_path_value.strip():
        image_path = Path(image_path_value.strip())
        logger.info("analyze_image: check fichero request_id=%s path=%s", request_id, image_path)
        if not image_path.exists() or not image_path.is_file():
            logger.warning("analyze_image: image_not_found request_id=%s path=%s", request_id, image_path)
            return jsonify(
                {"ok": False, "error": "image_not_found", "request_id": request_id, "image_path": str(image_path)},
            ), 404
    else:
        logger.warning("analyze_image: missing_image_input request_id=%s", request_id)
        return jsonify({"ok": False, "error": "missing_image_input", "request_id": request_id}), 400

    if not isinstance(task, str) or len(task.strip()) == 0:
        return jsonify({"ok": False, "error": "invalid_task", "request_id": request_id}), 400

    if not isinstance(context, str):
        return jsonify({"ok": False, "error": "invalid_context", "request_id": request_id}), 400

    try:
        logger.info("analyze_image: inicio lectura+llamada OpenAI request_id=%s", request_id)
        result = get_gateway().analyze_image(
            image_path=image_path,
            task=task.strip(),
            context=context.strip(),
            cultivo=cultivo,
            variedad=variedad,
        )
        elapsed = time.perf_counter() - start_ts
        logger.info("analyze_image: fin llamada OpenAI request_id=%s duracion=%.2fs", request_id, elapsed)
    except FileNotFoundError:
        logger.error("analyze_image: openai_key_file_missing request_id=%s", request_id)
        return jsonify({"ok": False, "error": "openai_key_file_missing", "request_id": request_id}), 500
    except ValueError as exc:
        # No exponemos detalles sensibles, pero sí diagnóstico útil.
        if "empty" in str(exc):
            logger.error("analyze_image: openai_key_empty request_id=%s", request_id)
            return jsonify({"ok": False, "error": "openai_key_empty", "request_id": request_id}), 500
        logger.error("analyze_image: invalid_config request_id=%s detail=%s", request_id, exc)
        return jsonify({"ok": False, "error": "invalid_config", "request_id": request_id}), 500
    except TimeoutError:
        elapsed = time.perf_counter() - start_ts
        logger.error("analyze_image: openai_timeout request_id=%s duracion=%.2fs", request_id, elapsed)
        return jsonify({"ok": False, "error": "openai_timeout", "request_id": request_id}), 504
    except OpenAIServiceError as exc:
        logger.error("analyze_image: openai_error request_id=%s detail=%s", request_id, exc)
        return jsonify({"ok": False, "error": "openai_error", "detail": str(exc), "request_id": request_id}), 502
    except Exception:  # pragma: no cover - fallback defensivo
        logger.error("analyze_image: internal_error request_id=%s\n%s", request_id, traceback.format_exc())
        return jsonify({"ok": False, "error": "internal_error", "request_id": request_id}), 500
    finally:
        if temporary_file_path and temporary_file_path.exists():
            try:
                temporary_file_path.unlink()
            except Exception:  # pragma: no cover - limpieza defensiva
                logger.warning("analyze_image: no se pudo borrar temporal request_id=%s path=%s", request_id, temporary_file_path)

    return jsonify({"ok": True, "result": result, "request_id": request_id})


if __name__ == "__main__":
    _configure_logging()
    cfg = get_config()
    app.run(host=cfg.bind_host, port=cfg.bind_port)
