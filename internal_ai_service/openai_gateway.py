"""Capa de acceso a OpenAI para mantener separada la lógica HTTP."""

from __future__ import annotations

import base64
import json
import logging
import os
import socket
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class OpenAIServiceError(RuntimeError):
    """Error controlado al hablar con OpenAI."""


class OpenAIGateway:
    PROMPTS_DB_ENV = "HARVESTSYNC_CALIBRES_IA_DB_PATH"
    PROMPTS_DB_ENV_LEGACY = "CALIBRES_IA_DB_PATH"
    PROMPTS_DB_DEFAULT_PATH = r"\\Personal\C\BasesSQLite\DBcalibres_ia.sqlite"

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
        prompts_db_raw = (
            os.getenv(self.PROMPTS_DB_ENV, "").strip()
            or os.getenv(self.PROMPTS_DB_ENV_LEGACY, "").strip()
            or self.PROMPTS_DB_DEFAULT_PATH
        )
        self.prompts_db_path = Path(prompts_db_raw)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _read_api_key(self) -> str:
        if not self.api_key_path.exists():
            raise FileNotFoundError(f"API key file not found: {self.api_key_path}")

        api_key = self.api_key_path.read_text(encoding="utf-8").strip()
        if not api_key:
            raise ValueError("API key is empty")

        return api_key

    @staticmethod
    def _fallback_prompt(task: str, context: str) -> str:
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
        if task == "estimacion_calibres":
            return (
                "Eres un asistente experto en estimación visual agrícola para naranjas en box.\n"
                "Objetivo: estimar una distribución APROXIMADA de calibres por foto, no una medición exacta fruto a fruto.\n"
                "Debes considerar explícitamente la oclusión, agrupación y perspectiva.\n"
                "No inventes precisión exacta ni afirmes certeza cuando no la hay.\n"
                "Usa los rangos de calibres provistos en el contexto como base de clasificación.\n"
                "Si la foto no permite una estimación razonable, devuelve apta_para_estimacion=false.\n"
                "Devuelve ÚNICAMENTE JSON válido, sin markdown ni texto extra, con este esquema exacto:\n"
                "{"
                "\"apta_para_estimacion\": true/false, "
                "\"confianza\": 0-100, "
                "\"frutos_visibles_estimados\": 0, "
                "\"calibre_dominante\": \"texto_o_null\", "
                "\"distribucion\": [{\"calibre\": \"texto\", \"porcentaje\": 0-100}], "
                "\"advertencias\": [\"...\"], "
                "\"resumen\": \"texto breve\""
                "}\n"
                "Reglas adicionales:\n"
                "- La suma de porcentajes en distribucion debe ser aproximadamente 100.\n"
                "- No inventes calibres fuera de los rangos enviados.\n"
                "- Si apta_para_estimacion=false, devuelve distribucion vacía o muy limitada y explica en advertencias.\n"
                f"Tarea={task}. Contexto={context or 'sin contexto'}"
            )

        base_prompt = (
            "Eres un asistente para control de calidad agrícola. "
            "Devuelve un JSON compacto con campos: summary, alerts, confidence."
        )
        return f"{base_prompt} Tarea={task}. Contexto={context or 'sin contexto'}"

    def ensure_prompt_schema(self) -> None:
        self.prompts_db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.prompts_db_path)) as conn:
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prompts_ia (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task TEXT NOT NULL,
                    cultivo TEXT NOT NULL,
                    variedad TEXT NOT NULL,
                    prompt_version TEXT NOT NULL,
                    nombre TEXT,
                    descripcion TEXT,
                    texto_prompt TEXT NOT NULL,
                    activo INTEGER DEFAULT 1,
                    es_default INTEGER DEFAULT 0,
                    fecha_creacion TEXT,
                    fecha_actualizacion TEXT
                )
                """
            )
            columns = [str(row[1]).strip().lower() for row in conn.execute("PRAGMA table_info(prompts_ia)").fetchall()]
            if "descripcion" not in columns:
                conn.execute("ALTER TABLE prompts_ia ADD COLUMN descripcion TEXT")
            if "es_default" not in columns:
                conn.execute("ALTER TABLE prompts_ia ADD COLUMN es_default INTEGER DEFAULT 0")
            if "fecha_modificacion" not in columns:
                conn.execute("ALTER TABLE prompts_ia ADD COLUMN fecha_modificacion TEXT")
            now = self._utc_now_iso()
            conn.execute(
                """
                UPDATE prompts_ia
                   SET fecha_modificacion = COALESCE(NULLIF(TRIM(fecha_modificacion), ''), fecha_actualizacion, fecha_creacion, ?)
                 WHERE fecha_modificacion IS NULL OR TRIM(fecha_modificacion) = ''
                """,
                (now,),
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_prompts_ia_task_cultivo_variedad_version "
                "ON prompts_ia(task, cultivo, variedad, prompt_version)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prompts_task ON prompts_ia(task)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prompts_cultivo ON prompts_ia(cultivo)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prompts_variedad ON prompts_ia(variedad)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prompts_activo ON prompts_ia(activo)")
            conn.commit()

    def seed_prompts_if_empty(self) -> None:
        now = self._utc_now_iso()
        with sqlite3.connect(str(self.prompts_db_path)) as conn:
            count = int(conn.execute("SELECT COUNT(*) FROM prompts_ia").fetchone()[0] or 0)
            if count > 0:
                return
            seed_rows = [
                (
                    "estimacion_calibres",
                    "CITRICOS",
                    "VALENCIA DELTA",
                    "v2_corrige_sesgo_valencia_delta",
                    "Estimación cítricos Valencia Delta (corrección sesgo)",
                    "Ajustado para corregir sobreestimación CAL8/CAL9 e infraestimación CAL4/CAL5.",
                    (
                        "Eres un asistente experto en estimación visual agrícola para cítricos en box.\n"
                        "Variedad objetivo: VALENCIA DELTA.\n"
                        "Objetivo: estimar distribución APROXIMADA de calibres por foto.\n"
                        "Corrige explícitamente el sesgo histórico:\n"
                        "- evitar sobreestimación de CAL8/CAL9;\n"
                        "- evitar infraestimación de CAL4/CAL5;\n"
                        "- manejar oclusión media y frutos parcialmente visibles sin invalidar automáticamente.\n"
                        "Usa solo calibres del contexto. Devuelve JSON estricto sin markdown con:\n"
                        "{"
                        "\"apta_para_estimacion\": true/false,"
                        "\"confianza\": 0-100,"
                        "\"frutos_visibles_estimados\": 0,"
                        "\"calibre_dominante\": \"texto_o_null\","
                        "\"distribucion\": [{\"calibre\": \"texto\", \"porcentaje\": 0-100}],"
                        "\"advertencias\": [\"...\"],"
                        "\"resumen\": \"texto breve\""
                        "}\n"
                        "Reglas: suma ~100, no inventar calibres fuera del contexto."
                    ),
                    1,
                    0,
                    now,
                    now,
                ),
                (
                    "estimacion_calibres",
                    "CITRICOS",
                    "*",
                    "v1_citricos_generico",
                    "Estimación cítricos genérico",
                    "Prompt base para cítricos cuando no hay ajuste específico de variedad.",
                    (
                        "Eres un asistente experto en estimación visual agrícola para cítricos en box.\n"
                        "Objetivo: estimar distribución APROXIMADA de calibres por foto, considerando oclusión y perspectiva.\n"
                        "Usa los rangos de calibres del contexto y evita precisión falsa.\n"
                        "Devuelve JSON estricto sin markdown con:\n"
                        "{"
                        "\"apta_para_estimacion\": true/false,"
                        "\"confianza\": 0-100,"
                        "\"frutos_visibles_estimados\": 0,"
                        "\"calibre_dominante\": \"texto_o_null\","
                        "\"distribucion\": [{\"calibre\": \"texto\", \"porcentaje\": 0-100}],"
                        "\"advertencias\": [\"...\"],"
                        "\"resumen\": \"texto breve\""
                        "}"
                    ),
                    1,
                    1,
                    now,
                    now,
                ),
                (
                    "estimacion_calibres",
                    "*",
                    "*",
                    "v1_generico_calibres",
                    "Estimación calibres genérico",
                    "Prompt genérico para cualquier cultivo sin configuración específica.",
                    (
                        "Eres un asistente de estimación visual agrícola en imágenes de fruta en box.\n"
                        "Objetivo: estimar distribución aproximada de calibres con cautela y advertencias claras.\n"
                        "Usa únicamente los rangos del contexto cuando estén disponibles.\n"
                        "Devuelve JSON estricto sin markdown con campos:\n"
                        "{"
                        "\"apta_para_estimacion\": true/false,"
                        "\"confianza\": 0-100,"
                        "\"frutos_visibles_estimados\": 0,"
                        "\"calibre_dominante\": \"texto_o_null\","
                        "\"distribucion\": [{\"calibre\": \"texto\", \"porcentaje\": 0-100}],"
                        "\"advertencias\": [\"...\"],"
                        "\"resumen\": \"texto breve\""
                        "}"
                    ),
                    1,
                    1,
                    now,
                    now,
                ),
            ]
            conn.executemany(
                """
                INSERT INTO prompts_ia (
                    task, cultivo, variedad, prompt_version, nombre, descripcion, texto_prompt,
                    activo, es_default, fecha_creacion, fecha_actualizacion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                seed_rows,
            )
            conn.commit()

    def resolve_prompt(
        self,
        *,
        task: str,
        cultivo: str,
        variedad: str,
        context: str,
    ) -> dict[str, str]:
        cultivo_norm = (cultivo or "*").strip().upper() or "*"
        variedad_norm = (variedad or "*").strip().upper() or "*"
        task_norm = task.strip().lower()

        self.ensure_prompt_schema()
        self.seed_prompts_if_empty()
        with sqlite3.connect(str(self.prompts_db_path)) as conn:
            conn.row_factory = sqlite3.Row
            exact_source = "sqlite_exact" if (cultivo_norm != "*" and variedad_norm != "*") else "sqlite_cultivo"
            levels = [(cultivo_norm, variedad_norm, exact_source)]
            if variedad_norm != "*":
                levels.append((cultivo_norm, "*", "sqlite_cultivo"))
            levels.append(("*", "*", "sqlite_generico"))
            for cultivo_q, variedad_q, source in levels:
                row = conn.execute(
                    """
                    SELECT texto_prompt, prompt_version
                    FROM prompts_ia
                    WHERE task = ? AND cultivo = ? AND variedad = ? AND activo = 1
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (task_norm, cultivo_q, variedad_q),
                ).fetchone()
                if row:
                    prompt = f"{row['texto_prompt']}\nTarea={task_norm}. Contexto={context or 'sin contexto'}"
                    return {
                        "prompt_text": prompt,
                        "prompt_version": str(row["prompt_version"]),
                        "prompt_source": source,
                        "cultivo": cultivo_norm,
                        "variedad": variedad_norm,
                    }
        return {
            "prompt_text": self._fallback_prompt(task_norm, context),
            "prompt_version": "fallback_internal_v1",
            "prompt_source": "fallback_internal",
            "cultivo": cultivo_norm,
            "variedad": variedad_norm,
        }

    def get_prompt_db_health(self) -> dict[str, Any]:
        db_exists = self.prompts_db_path.exists()
        table_exists = False
        prompts_count = 0
        try:
            with sqlite3.connect(str(self.prompts_db_path)) as conn:
                table_row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'prompts_ia' LIMIT 1"
                ).fetchone()
                table_exists = table_row is not None
                if table_exists:
                    prompts_count = int(conn.execute("SELECT COUNT(*) FROM prompts_ia").fetchone()[0] or 0)
        except Exception as exc:  # pragma: no cover - diagnóstico en health
            self.logger.warning("No se pudo consultar salud de prompts sqlite: %s", exc)
        return {
            "prompt_db_path": str(self.prompts_db_path),
            "prompt_db_exists": bool(db_exists),
            "prompts_table_exists": bool(table_exists),
            "prompts_count": int(prompts_count),
        }

    def analyze_image(self, *, image_path: Path, task: str, context: str, cultivo: str = "", variedad: str = "") -> dict[str, Any]:
        api_key = self._read_api_key()
        prompt_version = "fallback_internal_v1"
        prompt_source = "fallback_internal"
        cultivo_out = (cultivo or "*").strip().upper() or "*"
        variedad_out = (variedad or "*").strip().upper() or "*"
        prompt_text = self._fallback_prompt(task, context)
        try:
            resolved = self.resolve_prompt(task=task, cultivo=cultivo, variedad=variedad, context=context)
            prompt_text = resolved["prompt_text"]
            prompt_version = resolved["prompt_version"]
            prompt_source = resolved["prompt_source"]
            cultivo_out = resolved["cultivo"]
            variedad_out = resolved["variedad"]
        except Exception as exc:  # pragma: no cover - fallback defensivo
            self.logger.warning("prompt resolve fallback interno por error de sqlite: %s", exc)

        self.logger.info("gateway: inicio lectura imagen path=%s", image_path)
        image_bytes = image_path.read_bytes()
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        request_body = {
            "model": self.model,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt_text},
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
            "prompt_version": prompt_version,
            "prompt_source": prompt_source,
            "cultivo": cultivo_out,
            "variedad": variedad_out,
        }
