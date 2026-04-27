"""Herramienta independiente: obtención de imágenes para cálculo de calibres."""
from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import tkinter as tk
from firebase_admin import firestore
from tkinter import messagebox, ttk

from ui_utils import BaseToolWindow
from calibres_vision import (
    CircleDetectionResult,
    CirclePatternDetector,
    FruitCaliberAnalyzer,
    PhotoFruitAnalysisResult,
)
from client_examples.internal_ai_client import (
    InternalAIClientError,
    call_analyze_image,
)

try:
    from PIL import Image, ImageOps, ImageTk
except Exception:  # pragma: no cover - fallback cuando PIL no está disponible
    Image = None
    ImageOps = None
    ImageTk = None

COLLECTION_CONFIG = "Configuraciones"
DOCUMENT_CONFIG = "calibres"
LOGGER = logging.getLogger(__name__)
DB_FRUTA_PATH = r"X:\BasesSQLite\DBfruta.sqlite"
CALIBRES_IA_HISTORY_DB_ENV = "HARVESTSYNC_CALIBRES_IA_DB_PATH"
CALIBRES_IA_HISTORY_DB_DEFAULT_PATH = r"\\Personal\C\BasesSQLite\DBcalibres_ia.sqlite"
PROMPT_VERSION = "v1_estimacion_base"
CALIBRES_CALIBRADOR = [f"CAL {idx}" for idx in range(10)]
FILTRO_CALIBRADOR_CAMPANA = "2026"
FILTRO_CALIBRADOR_EMPRESA = "1"
FILTRO_CALIBRADOR_CULTIVO = "CITRICOS"
OPCION_BOLETA_COMPLETA = "Boleta completa"


def normalizar_distribucion_calibres(calibres_brutos: dict[str, float]) -> dict[str, float]:
    """Normaliza CAL 0..CAL 9 a 100% excluyendo podrido/destrío."""
    total_calibres = sum(max(0.0, float(v or 0.0)) for v in calibres_brutos.values())
    if total_calibres <= 0:
        raise ValueError("No se puede normalizar: suma de CAL 0..CAL 9 es 0.")
    return {cal: (max(0.0, float(valor or 0.0)) * 100.0 / total_calibres) for cal, valor in calibres_brutos.items()}


def comparar_distribuciones(
    distribucion_ia: dict[str, float],
    distribucion_real_normalizada: dict[str, float],
) -> dict[str, Any]:
    """Compara IA vs real normalizado por calibre."""
    calibres = sorted(set(distribucion_ia.keys()) | set(distribucion_real_normalizada.keys()))
    filas: list[dict[str, Any]] = []
    errores_abs: list[float] = []
    for calibre in calibres:
        pct_ia = max(0.0, float(distribucion_ia.get(calibre, 0.0) or 0.0))
        pct_real = max(0.0, float(distribucion_real_normalizada.get(calibre, 0.0) or 0.0))
        dif_abs = abs(pct_ia - pct_real)
        errores_abs.append(dif_abs)
        filas.append(
            {
                "calibre": calibre,
                "ia": pct_ia,
                "real_normalizado": pct_real,
                "diferencia_abs": dif_abs,
            }
        )

    dominante_ia = max(distribucion_ia.items(), key=lambda item: item[1])[0] if distribucion_ia else "-"
    dominante_real = (
        max(distribucion_real_normalizada.items(), key=lambda item: item[1])[0] if distribucion_real_normalizada else "-"
    )
    return {
        "filas": filas,
        "error_abs_medio": (sum(errores_abs) / len(errores_abs)) if errores_abs else 0.0,
        "error_total_abs": sum(errores_abs),
        "calibre_dominante_ia": dominante_ia,
        "calibre_dominante_real": dominante_real,
    }


def clasificar_calidad_error(error_abs_medio: float) -> str:
    """Clasifica la calidad de comparación usando error absoluto medio."""
    try:
        error = float(error_abs_medio)
    except (TypeError, ValueError):
        return "-"
    if error <= 5.0:
        return "BUENA"
    if error <= 10.0:
        return "ACEPTABLE"
    if error <= 15.0:
        return "MALA"
    return "MUY_MALA"


def calcular_sesgo_por_calibre(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Calcula sesgo medio IA-Real normalizado por CAL0..CAL9 sobre filas filtradas."""
    if not rows:
        return {
            "filas": [],
            "resumen": {
                "total_registros": 0,
                "calibre_mas_sobreestimado": "-",
                "sesgo_max": 0.0,
                "calibre_mas_infraestimado": "-",
                "sesgo_min": 0.0,
                "tendencia_general": "Sin datos",
            },
            "texto_resumen": "Sin registros para calcular sesgo por calibre.",
        }

    filas: list[dict[str, Any]] = []
    for idx in range(10):
        key_ia = f"ia_cal{idx}"
        key_real = f"real_norm_cal{idx}"
        media_ia = sum(float(row.get(key_ia) or 0.0) for row in rows) / len(rows)
        media_real = sum(float(row.get(key_real) or 0.0) for row in rows) / len(rows)
        sesgo = media_ia - media_real
        if sesgo > 5.0:
            interpretacion = "IA sobreestima"
        elif sesgo < -5.0:
            interpretacion = "IA infraestima"
        else:
            interpretacion = "Ajustado"
        filas.append(
            {
                "calibre": f"CAL {idx}",
                "media_ia": media_ia,
                "media_real": media_real,
                "sesgo": sesgo,
                "interpretacion": interpretacion,
            }
        )

    fila_max = max(filas, key=lambda item: item["sesgo"])
    fila_min = min(filas, key=lambda item: item["sesgo"])
    sesgo_global = sum(item["sesgo"] for item in filas) / len(filas)
    if sesgo_global > 1.0:
        tendencia = "Desplazamiento global de IA hacia mayor porcentaje en calibres analizados"
    elif sesgo_global < -1.0:
        tendencia = "Desplazamiento global de IA hacia menor porcentaje en calibres analizados"
    else:
        tendencia = "Sin desplazamiento global relevante"

    resumen = {
        "total_registros": len(rows),
        "calibre_mas_sobreestimado": fila_max["calibre"],
        "sesgo_max": fila_max["sesgo"],
        "calibre_mas_infraestimado": fila_min["calibre"],
        "sesgo_min": fila_min["sesgo"],
        "tendencia_general": tendencia,
    }
    texto_resumen = (
        "[Sesgo IA por calibre]\n"
        f"Registros analizados: {resumen['total_registros']}\n"
        f"Más sobreestimado: {resumen['calibre_mas_sobreestimado']} "
        f"(sesgo IA-Real={resumen['sesgo_max']:.2f} pp)\n"
        f"Más infraestimado: {resumen['calibre_mas_infraestimado']} "
        f"(sesgo IA-Real={resumen['sesgo_min']:.2f} pp)\n"
        f"Tendencia general: {resumen['tendencia_general']}"
    )
    return {"filas": filas, "resumen": resumen, "texto_resumen": texto_resumen}


def calcular_puntuacion_version(row_version: dict[str, Any]) -> float:
    """Calcula puntuación operativa de una versión de prompt IA."""
    error_medio = float(row_version.get("error_medio_global") or 0.0)
    error_total_medio = float(row_version.get("error_total_medio") or 0.0)
    total_buena = int(row_version.get("total_buena") or 0)
    total_aceptable = int(row_version.get("total_aceptable") or 0)
    total_mala = int(row_version.get("total_mala") or 0)
    total_muy_mala = int(row_version.get("total_muy_mala") or 0)
    registros = int(row_version.get("numero_registros") or 0)
    dominante_ia = str(row_version.get("dominante_ia_mas_frecuente", "-") or "-").strip()
    dominante_real = str(row_version.get("dominante_real_mas_frecuente", "-") or "-").strip()

    puntuacion = 100.0
    puntuacion -= error_medio * 5.0
    puntuacion -= error_total_medio * 0.2
    puntuacion += total_buena * 3.0
    puntuacion += total_aceptable * 1.0
    puntuacion -= total_mala * 5.0
    puntuacion -= total_muy_mala * 10.0
    if dominante_ia and dominante_real and dominante_ia == dominante_real:
        puntuacion += 10.0
    if registros < 3:
        puntuacion -= 20.0
    return puntuacion


def generar_recomendacion_versiones(summary_by_version: list[dict[str, Any]]) -> dict[str, str]:
    """Genera resumen de decisión para comparativa por versión."""
    versiones_con_datos = [row for row in summary_by_version if int(row.get("numero_registros") or 0) > 0]
    if not versiones_con_datos:
        return {
            "mensaje": "No hay suficientes versiones para comparar.",
            "copiable": "No hay suficientes versiones para comparar.",
        }

    mejor_error_medio = min(versiones_con_datos, key=lambda row: float(row.get("error_medio_global") or 0.0))
    mejor_error_total = min(versiones_con_datos, key=lambda row: float(row.get("error_total_medio") or 0.0))
    mejor_calidad = max(
        versiones_con_datos,
        key=lambda row: (
            int(row.get("total_buena") or 0),
            -int(row.get("total_muy_mala") or 0),
            -int(row.get("total_mala") or 0),
            int(row.get("total_aceptable") or 0),
        ),
    )

    filas_puntuadas = [
        {
            **row,
            "puntuacion": calcular_puntuacion_version(row),
        }
        for row in versiones_con_datos
    ]
    filas_puntuadas.sort(key=lambda row: float(row.get("puntuacion") or 0.0), reverse=True)
    recomendada = filas_puntuadas[0]

    if len(filas_puntuadas) == 1:
        version = str(recomendada.get("prompt_version", "sin_version") or "sin_version")
        mensaje_unico = (
            "Solo hay una versión con datos. No se puede comparar, pero se muestra como versión actual.\n"
            f"Versión actual: {version}."
        )
        return {"mensaje": mensaje_unico, "copiable": mensaje_unico}

    diferencia_top2 = float(filas_puntuadas[0].get("puntuacion") or 0.0) - float(filas_puntuadas[1].get("puntuacion") or 0.0)
    no_concluyente = diferencia_top2 < 5.0

    motivos: list[str] = []
    if recomendada["prompt_version"] == mejor_error_medio["prompt_version"]:
        motivos.append("menor error medio")
    if recomendada["prompt_version"] == mejor_error_total["prompt_version"]:
        motivos.append("menor error total medio")
    if (
        str(recomendada.get("dominante_ia_mas_frecuente", "-"))
        == str(recomendada.get("dominante_real_mas_frecuente", "-"))
    ):
        motivos.append("dominante IA coincide con real")
    if int(recomendada.get("total_mala") or 0) == 0 and int(recomendada.get("total_muy_mala") or 0) == 0:
        motivos.append("sin registros MALA ni MUY_MALA")
    if recomendada["prompt_version"] == mejor_calidad["prompt_version"]:
        motivos.append("mejor balance de calidad")

    advertencias: list[str] = []
    for row in versiones_con_datos:
        if int(row.get("numero_registros") or 0) < 3:
            advertencias.append(f"Advertencia: versión con muestra insuficiente ({row.get('prompt_version', 'sin_version')}).")

    version_error_medio = str(mejor_error_medio.get("prompt_version", "sin_version") or "sin_version")
    version_error_total = str(mejor_error_total.get("prompt_version", "sin_version") or "sin_version")
    version_calidad = str(mejor_calidad.get("prompt_version", "sin_version") or "sin_version")
    version_recomendada = str(recomendada.get("prompt_version", "sin_version") or "sin_version")
    motivo_texto = ", ".join(motivos) if motivos else "mayor puntuación compuesta"

    lineas = [
        f"Mejor por error medio: {version_error_medio}",
        f"Mejor por error total medio: {version_error_total}",
        f"Mejor por calidad: {version_calidad}",
    ]
    if no_concluyente:
        lineas.append("Versión recomendada: Recomendación no concluyente.")
        lineas.append("Motivo: diferencias pequeñas entre versiones.")
    else:
        lineas.append(f"Versión recomendada: {version_recomendada}")
        lineas.append(f"Motivo: {motivo_texto}.")
    lineas.extend(advertencias)
    texto = "\n".join(lineas)
    return {"mensaje": texto, "copiable": texto}


def _valor_a_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        limpio = value.strip().replace("%", "").replace(",", ".")
        try:
            return float(limpio)
        except ValueError:
            return default
    return default


def _normalizar_porcentaje(value: Any) -> float:
    num = _valor_a_float(value, default=0.0)
    if num < 0:
        return 0.0
    # Soporta fracciones 0..1 y porcentaje 0..100.
    return num * 100.0 if num <= 1.0 else num


def listar_entregas_por_boleta(
    db_path: str,
    boleta: str,
    campana: str = FILTRO_CALIBRADOR_CAMPANA,
    empresa: str = FILTRO_CALIBRADOR_EMPRESA,
    cultivo: str = FILTRO_CALIBRADOR_CULTIVO,
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    try:
        sql = """
        SELECT
            TRIM(COALESCE(Boleta, '')) AS Boleta,
            TRIM(COALESCE(CAMPAÑA, '')) AS Campana,
            TRIM(COALESCE(EMPRESA, '')) AS Empresa,
            TRIM(COALESCE(CULTIVO, '')) AS Cultivo,
            TRIM(COALESCE(AlbaranDef, '')) AS AlbaranDef,
            TRIM(COALESCE(Albaran, '')) AS Albaran,
            TRIM(COALESCE("Albarán2", '')) AS Albaran2,
            COALESCE(Neto, 0) AS Neto,
            COALESCE(Fcarga, '') AS Fcarga,
            COALESCE("FRecolección", '') AS FRecoleccion,
            TRIM(COALESCE(Variedad, '')) AS Variedad,
            TRIM(COALESCE(Socio, '')) AS Socio,
            TRIM(COALESCE(IdSocio, '')) AS IdSocio,
            COALESCE("%Podrido", 0) AS Podrido,
            COALESCE("%DesLinea", 0) AS DesLinea,
            COALESCE("%DesMesa", 0) AS DesMesa,
            COALESCE("%Cal0", 0) AS Cal0,
            COALESCE("%Cal1", 0) AS Cal1,
            COALESCE("%Cal2", 0) AS Cal2,
            COALESCE("%Cal3", 0) AS Cal3,
            COALESCE("%Cal4", 0) AS Cal4,
            COALESCE("%Cal5", 0) AS Cal5,
            COALESCE("%Cal6", 0) AS Cal6,
            COALESCE("%Cal7", 0) AS Cal7,
            COALESCE("%Cal8", 0) AS Cal8,
            COALESCE("%Cal9", 0) AS Cal9
        FROM PesosFres
        WHERE TRIM(COALESCE(Boleta, '')) = ?
          AND TRIM(COALESCE(CAMPAÑA, '')) = ?
          AND TRIM(COALESCE(EMPRESA, '')) = ?
          AND UPPER(TRIM(COALESCE(CULTIVO, ''))) = ?
        ORDER BY DATE(COALESCE(Fcarga, '')) ASC, TRIM(COALESCE(AlbaranDef, Albaran, '')) ASC
        """
        rows = conn.execute(sql, (boleta, campana, empresa, cultivo.upper())).fetchall()
    finally:
        conn.close()

    entregas: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        entrega = {key: row[key] for key in row.keys()}
        entrega["idx"] = idx
        entrega["neto"] = _valor_a_float(row["Neto"], 0.0)
        entregas.append(entrega)
    return entregas


def cargar_calibrador_por_entrega(entrega: dict[str, Any]) -> dict[str, Any]:
    pod = _normalizar_porcentaje(entrega.get("Podrido"))
    des_linea = _normalizar_porcentaje(entrega.get("DesLinea"))
    des_mesa = _normalizar_porcentaje(entrega.get("DesMesa"))
    calibres_brutos = {f"CAL {idx}": _normalizar_porcentaje(entrega.get(f"Cal{idx}")) for idx in range(10)}
    return {
        "podrido": pod,
        "des_linea": des_linea,
        "des_mesa": des_mesa,
        "destrio_total": des_linea + des_mesa,
        "calibres_brutos": calibres_brutos,
        "suma_calibres": sum(calibres_brutos.values()),
    }


def cargar_calibrador_boleta_ponderado(entregas: list[dict[str, Any]]) -> dict[str, Any]:
    if not entregas:
        raise ValueError("No hay entregas disponibles para calcular boleta completa.")
    peso_total = sum(max(0.0, _valor_a_float(item.get("neto"), 0.0)) for item in entregas)
    if peso_total <= 0:
        raise ValueError("No se puede ponderar boleta completa: suma de Neto <= 0.")

    campos_pct = ["Podrido", "DesLinea", "DesMesa", *[f"Cal{idx}" for idx in range(10)]]
    ponderados: dict[str, float] = {campo: 0.0 for campo in campos_pct}
    for item in entregas:
        neto = max(0.0, _valor_a_float(item.get("neto"), 0.0))
        for campo in campos_pct:
            ponderados[campo] += neto * _normalizar_porcentaje(item.get(campo))

    pod = ponderados["Podrido"] / peso_total
    des_linea = ponderados["DesLinea"] / peso_total
    des_mesa = ponderados["DesMesa"] / peso_total
    calibres_brutos = {f"CAL {idx}": ponderados[f"Cal{idx}"] / peso_total for idx in range(10)}
    return {
        "podrido": pod,
        "des_linea": des_linea,
        "des_mesa": des_mesa,
        "destrio_total": des_linea + des_mesa,
        "calibres_brutos": calibres_brutos,
        "suma_calibres": sum(calibres_brutos.values()),
    }


@dataclass
class CalibresConfig:
    """Configuración funcional para la obtención y posterior cálculo de calibres."""

    diametro_patron_mm: float
    pantalla_fotos: str
    rangos_por_cultivo: dict[str, list[dict[str, Any]]]


class CalibresConfigRepository:
    """Acceso a configuración persistida de calibres en Firestore."""

    def __init__(self, db: firestore.Client) -> None:
        self.db = db

    def load(self) -> CalibresConfig:
        doc = self.db.collection(COLLECTION_CONFIG).document(DOCUMENT_CONFIG).get()
        data = doc.to_dict() if doc.exists else {}
        diametro = float(data.get("diametro_patron_mm", 94.0) or 94.0)
        pantalla = str(data.get("pantalla_fotos", "Datos Calibres") or "Datos Calibres").strip()
        rangos = data.get("rangos", []) or []

        rangos_por_cultivo: dict[str, list[dict[str, Any]]] = {}
        for row in rangos:
            cultivo = str(row.get("cultivo", "")).strip()
            if not cultivo:
                continue
            rangos_por_cultivo.setdefault(cultivo, []).append(
                {
                    "nombre_calibre": str(row.get("nombre_calibre", "")).strip(),
                    "desde_mm": float(row.get("desde_mm", 0.0) or 0.0),
                    "hasta_mm": float(row.get("hasta_mm", 0.0) or 0.0),
                    "orden": int(row.get("orden", 0) or 0),
                }
            )

        for cultivo, filas in rangos_por_cultivo.items():
            filas.sort(key=lambda item: (item["orden"], item["desde_mm"], item["hasta_mm"]))

        return CalibresConfig(
            diametro_patron_mm=diametro,
            pantalla_fotos=pantalla or "Datos Calibres",
            rangos_por_cultivo=rangos_por_cultivo,
        )


class CalibresIAHistoryRepository:
    """Persistencia local SQLite para histórico IA vs calibrador."""

    def __init__(self, db_path: str | None = None) -> None:
        configured = (db_path or os.getenv(CALIBRES_IA_HISTORY_DB_ENV, "")).strip()
        self.db_path = self._resolve_db_path(configured or CALIBRES_IA_HISTORY_DB_DEFAULT_PATH)
        self._initialized = False
        self._lock = threading.Lock()

    @staticmethod
    def _resolve_db_path(raw_path: str) -> str:
        path = str(raw_path or "").strip()
        if path.startswith("\\") and not path.startswith("\\\\"):
            raise ValueError(
                "Ruta UNC mal formada para histórico IA: debe iniciar con doble barra (\\\\servidor\\recurso)."
            )
        return path

    @staticmethod
    def _get_parent_dir(db_path: str) -> str:
        return os.path.dirname(db_path)

    def _validate_parent_dir_exists(self) -> tuple[str, bool]:
        parent_dir = self._get_parent_dir(self.db_path)
        if not parent_dir:
            raise ValueError(f"No se pudo resolver la carpeta padre del histórico IA: {self.db_path}")
        parent_exists = os.path.isdir(parent_dir)
        LOGGER.info("Histórico IA - ruta final resuelta: %s", self.db_path)
        LOGGER.info("Histórico IA - carpeta padre existe=%s path=%s", parent_exists, parent_dir)
        print(f"[CalibresIAHistoryRepository] Ruta final resuelta: {self.db_path}")
        print(f"[CalibresIAHistoryRepository] Carpeta padre existe: {parent_exists} ({parent_dir})")
        if not parent_exists:
            raise FileNotFoundError(
                "No existe la carpeta padre para guardar histórico IA: "
                f"{parent_dir}. Revise la ruta UNC en {CALIBRES_IA_HISTORY_DB_ENV}."
            )
        return parent_dir, parent_exists

    @staticmethod
    def ensure_column_exists(
        conn: sqlite3.Connection, table_name: str, column_name: str, column_definition: str
    ) -> None:
        cols = {str(row[1]).strip().lower() for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name.strip().lower() not in cols:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")

    def ensure_schema(self) -> None:
        with self._lock:
            if self._initialized:
                return
            self._validate_parent_dir_exists()
            db_exists_before = os.path.exists(self.db_path)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA busy_timeout = 5000;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS comparaciones_calibres (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fecha_registro TEXT NOT NULL,
                        boleta INTEGER,
                        albaran TEXT,
                        tipo_comparacion TEXT,
                        campana INTEGER,
                        empresa INTEGER,
                        cultivo TEXT,
                        variedad TEXT,
                        socio TEXT,
                        id_socio INTEGER,
                        neto REAL,
                        id_foto TEXT,
                        image_url TEXT,
                        ruta_local TEXT,
                        modelo_ia TEXT,
                        prompt_version TEXT,
                        prompt_source TEXT,
                        confianza_ia REAL,
                        calibre_dominante_ia TEXT,
                        calibre_dominante_real TEXT,
                        error_absoluto_medio REAL,
                        error_total_absoluto REAL,
                        podrido REAL,
                        deslinea REAL,
                        desmesa REAL,
                        destrio_total REAL,
                        suma_calibres_real REAL,
                        ia_cal0 REAL,
                        ia_cal1 REAL,
                        ia_cal2 REAL,
                        ia_cal3 REAL,
                        ia_cal4 REAL,
                        ia_cal5 REAL,
                        ia_cal6 REAL,
                        ia_cal7 REAL,
                        ia_cal8 REAL,
                        ia_cal9 REAL,
                        real_norm_cal0 REAL,
                        real_norm_cal1 REAL,
                        real_norm_cal2 REAL,
                        real_norm_cal3 REAL,
                        real_norm_cal4 REAL,
                        real_norm_cal5 REAL,
                        real_norm_cal6 REAL,
                        real_norm_cal7 REAL,
                        real_norm_cal8 REAL,
                        real_norm_cal9 REAL,
                        real_bruto_cal0 REAL,
                        real_bruto_cal1 REAL,
                        real_bruto_cal2 REAL,
                        real_bruto_cal3 REAL,
                        real_bruto_cal4 REAL,
                        real_bruto_cal5 REAL,
                        real_bruto_cal6 REAL,
                        real_bruto_cal7 REAL,
                        real_bruto_cal8 REAL,
                        real_bruto_cal9 REAL,
                        advertencias_ia TEXT,
                        resumen_ia TEXT,
                        output_ia_json TEXT,
                        observaciones TEXT
                    )
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_calibres_boleta ON comparaciones_calibres (boleta)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_calibres_albaran ON comparaciones_calibres (albaran)")
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_comp_calibres_fecha_registro ON comparaciones_calibres (fecha_registro)"
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_calibres_variedad ON comparaciones_calibres (variedad)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_calibres_cultivo ON comparaciones_calibres (cultivo)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_calibres_prompt_version ON comparaciones_calibres (prompt_version)")
                self.ensure_column_exists(conn, "comparaciones_calibres", "cultivo", "TEXT")
                self.ensure_column_exists(conn, "comparaciones_calibres", "variedad", "TEXT")
                self.ensure_column_exists(conn, "comparaciones_calibres", "prompt_version", "TEXT")
                self.ensure_column_exists(conn, "comparaciones_calibres", "prompt_source", "TEXT")
                conn.execute("DROP INDEX IF EXISTS ux_comp_calibres_dedupe")
                conn.commit()
            db_exists_after = os.path.exists(self.db_path)
            LOGGER.info(
                "Histórico IA - archivo sqlite previo=%s actual=%s path=%s",
                db_exists_before,
                db_exists_after,
                self.db_path,
            )
            print(
                "[CalibresIAHistoryRepository] Archivo sqlite "
                f"previo={db_exists_before} actual={db_exists_after} ({self.db_path})"
            )
            self._initialized = True

    def save_comparison(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        self.ensure_schema()
        columns = [
            "fecha_registro",
            "boleta",
            "albaran",
            "tipo_comparacion",
            "campana",
            "empresa",
            "cultivo",
            "variedad",
            "socio",
            "id_socio",
            "neto",
            "id_foto",
            "image_url",
            "ruta_local",
            "modelo_ia",
            "prompt_version",
            "prompt_source",
            "confianza_ia",
            "calibre_dominante_ia",
            "calibre_dominante_real",
            "error_absoluto_medio",
            "error_total_absoluto",
            "podrido",
            "deslinea",
            "desmesa",
            "destrio_total",
            "suma_calibres_real",
            *[f"ia_cal{idx}" for idx in range(10)],
            *[f"real_norm_cal{idx}" for idx in range(10)],
            *[f"real_bruto_cal{idx}" for idx in range(10)],
            "advertencias_ia",
            "resumen_ia",
            "output_ia_json",
            "observaciones",
        ]
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO comparaciones_calibres ({', '.join(columns)}) VALUES ({placeholders})"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA busy_timeout = 5000;")
            payload = [tuple(row.get(col) for col in columns) for row in rows]
            conn.executemany(sql, payload)
            conn.commit()
            return len(rows)

    def _connect_readonly(self) -> sqlite3.Connection:
        parent_dir, parent_exists = self._validate_parent_dir_exists()
        if not parent_exists:
            raise FileNotFoundError(
                "No existe la carpeta de la base histórica IA: "
                f"{parent_dir}. Revise la ruta UNC en {CALIBRES_IA_HISTORY_DB_ENV}."
            )
        if not os.path.isfile(self.db_path):
            raise FileNotFoundError("No existe la base histórica. Guarde primero una comparación.")

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    @staticmethod
    def _parse_tabla_no_existe(exc: sqlite3.OperationalError) -> None:
        if "no such table" in str(exc).lower():
            raise LookupError("La tabla comparaciones_calibres no existe en la base histórica IA.") from exc
        raise exc

    @staticmethod
    def _build_filtros_sql(
        *,
        boleta: str = "",
        albaran: str = "",
        variedad: str = "",
        cultivo: str = "",
        calidad: str = "",
        prompt_version: str = "",
    ) -> tuple[list[str], list[Any]]:
        filtros_sql: list[str] = []
        params: list[Any] = []
        if boleta.strip():
            filtros_sql.append("CAST(COALESCE(boleta, '') AS TEXT) LIKE ?")
            params.append(f"%{boleta.strip()}%")
        if albaran.strip():
            filtros_sql.append("UPPER(COALESCE(albaran, '')) LIKE UPPER(?)")
            params.append(f"%{albaran.strip()}%")
        if variedad.strip():
            filtros_sql.append("UPPER(COALESCE(variedad, '')) LIKE UPPER(?)")
            params.append(f"%{variedad.strip()}%")
        if cultivo.strip():
            filtros_sql.append("UPPER(COALESCE(cultivo, '')) LIKE UPPER(?)")
            params.append(f"%{cultivo.strip()}%")
        calidad_limpia = calidad.strip().upper()
        if calidad_limpia in {"BUENA", "ACEPTABLE", "MALA", "MUY_MALA"}:
            filtros_sql.append(
                """(
                    CASE
                        WHEN COALESCE(error_absoluto_medio, 999999) <= 5 THEN 'BUENA'
                        WHEN COALESCE(error_absoluto_medio, 999999) <= 10 THEN 'ACEPTABLE'
                        WHEN COALESCE(error_absoluto_medio, 999999) <= 15 THEN 'MALA'
                        ELSE 'MUY_MALA'
                    END
                ) = ?"""
            )
            params.append(calidad_limpia)
        version_limpia = prompt_version.strip()
        if version_limpia and version_limpia.upper() != "TODAS":
            filtros_sql.append("COALESCE(NULLIF(TRIM(prompt_version), ''), 'sin_version') = ?")
            params.append(version_limpia)
        return filtros_sql, params

    def list_comparisons(
        self,
        *,
        boleta: str = "",
        albaran: str = "",
        variedad: str = "",
        cultivo: str = "",
        calidad: str = "",
        prompt_version: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        filtros_sql, params = self._build_filtros_sql(
            boleta=boleta,
            albaran=albaran,
            variedad=variedad,
            cultivo=cultivo,
            calidad=calidad,
            prompt_version=prompt_version,
        )
        where_sql = f"WHERE {' AND '.join(filtros_sql)}" if filtros_sql else ""
        sql = f"""
            SELECT
                id,
                fecha_registro,
                boleta,
                albaran,
                variedad,
                cultivo,
                id_foto,
                modelo_ia,
                COALESCE(NULLIF(TRIM(prompt_version), ''), 'sin_version') AS prompt_version,
                COALESCE(NULLIF(TRIM(prompt_source), ''), 'no_informado') AS prompt_source,
                confianza_ia,
                calibre_dominante_ia,
                calibre_dominante_real,
                error_absoluto_medio,
                error_total_absoluto,
                CASE
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 5 THEN 'BUENA'
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 10 THEN 'ACEPTABLE'
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 15 THEN 'MALA'
                    ELSE 'MUY_MALA'
                END AS calidad
            FROM comparaciones_calibres
            {where_sql}
            ORDER BY datetime(fecha_registro) DESC, id DESC
            LIMIT ?
        """
        params.append(max(1, int(limit)))
        try:
            with self._connect_readonly() as conn:
                rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            self._parse_tabla_no_existe(exc)
        return [dict(row) for row in rows]

    def list_comparisons_for_bias(
        self,
        *,
        boleta: str = "",
        albaran: str = "",
        variedad: str = "",
        cultivo: str = "",
        calidad: str = "",
        prompt_version: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        self.ensure_schema()
        filtros_sql, params = self._build_filtros_sql(
            boleta=boleta,
            albaran=albaran,
            variedad=variedad,
            cultivo=cultivo,
            calidad=calidad,
            prompt_version=prompt_version,
        )
        where_sql = f"WHERE {' AND '.join(filtros_sql)}" if filtros_sql else ""
        sql = f"""
            SELECT
                id,
                fecha_registro,
                boleta,
                albaran,
                variedad,
                cultivo,
                id_foto,
                modelo_ia,
                COALESCE(NULLIF(TRIM(prompt_version), ''), 'sin_version') AS prompt_version,
                COALESCE(NULLIF(TRIM(prompt_source), ''), 'no_informado') AS prompt_source,
                confianza_ia,
                calibre_dominante_ia,
                calibre_dominante_real,
                error_absoluto_medio,
                error_total_absoluto,
                CASE
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 5 THEN 'BUENA'
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 10 THEN 'ACEPTABLE'
                    WHEN COALESCE(error_absoluto_medio, 999999) <= 15 THEN 'MALA'
                    ELSE 'MUY_MALA'
                END AS calidad,
                ia_cal0, ia_cal1, ia_cal2, ia_cal3, ia_cal4, ia_cal5, ia_cal6, ia_cal7, ia_cal8, ia_cal9,
                real_norm_cal0, real_norm_cal1, real_norm_cal2, real_norm_cal3, real_norm_cal4,
                real_norm_cal5, real_norm_cal6, real_norm_cal7, real_norm_cal8, real_norm_cal9
            FROM comparaciones_calibres
            {where_sql}
            ORDER BY datetime(fecha_registro) DESC, id DESC
            LIMIT ?
        """
        params.append(max(1, int(limit)))
        try:
            with self._connect_readonly() as conn:
                rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            self._parse_tabla_no_existe(exc)
        return [dict(row) for row in rows]

    def get_comparison_detail(self, comparison_id: int) -> dict[str, Any] | None:
        self.ensure_schema()
        sql = """
            SELECT *
            FROM comparaciones_calibres
            WHERE id = ?
            LIMIT 1
        """
        try:
            with self._connect_readonly() as conn:
                row = conn.execute(sql, (comparison_id,)).fetchone()
        except sqlite3.OperationalError as exc:
            self._parse_tabla_no_existe(exc)
        return dict(row) if row else None

    def get_summary(
        self,
        *,
        boleta: str = "",
        albaran: str = "",
        variedad: str = "",
        cultivo: str = "",
        calidad: str = "",
        prompt_version: str = "",
    ) -> dict[str, Any]:
        self.ensure_schema()
        filtros_sql, params = self._build_filtros_sql(
            boleta=boleta,
            albaran=albaran,
            variedad=variedad,
            cultivo=cultivo,
            calidad=calidad,
            prompt_version=prompt_version,
        )
        where_sql = f"WHERE {' AND '.join(filtros_sql)}" if filtros_sql else ""
        sql_base = f"""
            SELECT
                COUNT(*) AS total_registros,
                AVG(COALESCE(error_absoluto_medio, 0)) AS error_medio_global,
                AVG(COALESCE(error_total_absoluto, 0)) AS error_total_medio,
                SUM(CASE WHEN COALESCE(error_absoluto_medio, 999999) <= 5 THEN 1 ELSE 0 END) AS total_buena,
                SUM(
                    CASE
                        WHEN COALESCE(error_absoluto_medio, 999999) > 5
                        AND COALESCE(error_absoluto_medio, 999999) <= 10 THEN 1
                        ELSE 0
                    END
                ) AS total_aceptable,
                SUM(
                    CASE
                        WHEN COALESCE(error_absoluto_medio, 999999) > 10
                        AND COALESCE(error_absoluto_medio, 999999) <= 15 THEN 1
                        ELSE 0
                    END
                ) AS total_mala,
                SUM(CASE WHEN COALESCE(error_absoluto_medio, 999999) > 15 THEN 1 ELSE 0 END) AS total_muy_mala
            FROM comparaciones_calibres
            {where_sql}
        """
        sql_dom_ia = f"""
            SELECT calibre_dominante_ia, COUNT(*) AS total
            FROM comparaciones_calibres
            {where_sql}
            GROUP BY calibre_dominante_ia
            ORDER BY total DESC, calibre_dominante_ia ASC
            LIMIT 1
        """
        sql_dom_real = f"""
            SELECT calibre_dominante_real, COUNT(*) AS total
            FROM comparaciones_calibres
            {where_sql}
            GROUP BY calibre_dominante_real
            ORDER BY total DESC, calibre_dominante_real ASC
            LIMIT 1
        """
        try:
            with self._connect_readonly() as conn:
                base = conn.execute(sql_base, params).fetchone()
                dom_ia = conn.execute(sql_dom_ia, params).fetchone()
                dom_real = conn.execute(sql_dom_real, params).fetchone()
        except sqlite3.OperationalError as exc:
            self._parse_tabla_no_existe(exc)

        return {
            "numero_registros": int(base["total_registros"] or 0),
            "error_medio_global": float(base["error_medio_global"] or 0.0),
            "error_total_medio": float(base["error_total_medio"] or 0.0),
            "dominante_ia_mas_frecuente": str((dom_ia["calibre_dominante_ia"] if dom_ia else "") or "-"),
            "dominante_real_mas_frecuente": str((dom_real["calibre_dominante_real"] if dom_real else "") or "-"),
            "total_buena": int(base["total_buena"] or 0),
            "total_aceptable": int(base["total_aceptable"] or 0),
            "total_mala": int(base["total_mala"] or 0),
            "total_muy_mala": int(base["total_muy_mala"] or 0),
        }

    def list_prompt_versions(self) -> list[str]:
        self.ensure_schema()
        sql = """
            SELECT DISTINCT COALESCE(NULLIF(TRIM(prompt_version), ''), 'sin_version') AS version
            FROM comparaciones_calibres
            ORDER BY version ASC
        """
        try:
            with self._connect_readonly() as conn:
                rows = conn.execute(sql).fetchall()
        except sqlite3.OperationalError as exc:
            self._parse_tabla_no_existe(exc)
        return [str(row["version"]) for row in rows if str(row["version"]).strip()]

    def get_summary_by_version(self, **filtros: Any) -> list[dict[str, Any]]:
        base = dict(filtros)
        prompt_version_filtro = str(base.get("prompt_version", "") or "").strip()
        base["prompt_version"] = ""
        if prompt_version_filtro and prompt_version_filtro.upper() != "TODAS":
            versiones = [prompt_version_filtro]
        else:
            versiones = self.list_prompt_versions()
        resumenes: list[dict[str, Any]] = []
        for version in versiones:
            query = dict(base)
            query["prompt_version"] = version
            resumen = self.get_summary(**query)
            if int(resumen.get("numero_registros") or 0) <= 0:
                continue
            resumen["prompt_version"] = version
            resumenes.append(resumen)
        return resumenes


class CalibresDataService:
    """Consultas a Firestore + servidor de fotos reutilizando el patrón actual."""

    def __init__(self, db: firestore.Client) -> None:
        self.db = db

    def get_muestras_by_boleta(self, boleta: str) -> list[dict[str, Any]]:
        docs = (
            self.db.collection("Muestras")
            .where("Boleta", "==", boleta)
            .stream()
        )
        muestras: list[dict[str, Any]] = []
        for doc in docs:
            data = doc.to_dict() or {}
            muestras.append(
                {
                    "id_muestra": doc.id,
                    "boleta": str(data.get("Boleta", "")).strip(),
                    "nombre": str(data.get("Nombre", "")).strip(),
                    "cultivo": str(data.get("CULTIVO", "")).strip(),
                    "fecha_hora": data.get("FechaHora"),
                }
            )

        muestras.sort(key=lambda item: str(item.get("fecha_hora") or ""), reverse=True)
        return muestras

    def get_fotos_by_muestra(self, id_muestra: str, pantalla: str) -> list[dict[str, Any]]:
        docs = (
            self.db.collection("Fotos")
            .where("idMuestra", "==", id_muestra)
            .where("pantalla", "==", pantalla)
            .order_by("timestamp")
            .stream()
        )

        fotos: list[dict[str, Any]] = []
        for doc in docs:
            data = doc.to_dict() or {}
            ruta_local = str(data.get("ruta_local", "")).strip()
            if not ruta_local:
                continue
            fotos.append(
                {
                    "id_foto": doc.id,
                    "id_muestra": id_muestra,
                    "pantalla": str(data.get("pantalla", "")).strip(),
                    "ruta_local": ruta_local,
                    "timestamp": data.get("timestamp"),
                }
            )
        return fotos

    def get_url_base_servidor_fotos(self) -> str:
        doc = self.db.collection("ServidorFotos").document("url_actual").get()
        data = doc.to_dict() if doc.exists else {}
        return str(data.get("url", "") or "").rstrip("/")

    def descargar_imagen(self, url: str, timeout: int = 8) -> bytes:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.content

    def resolve_url_servicio_ia(self) -> tuple[str, str, str | None]:
        """Resuelve URL del servicio IA y devuelve (url, origen, error)."""
        env_url = os.getenv("HARVESTSYNC_INTERNAL_AI_URL", "").strip().rstrip("/")
        if env_url:
            LOGGER.info("Validación IA: URL de servicio resuelta desde variable de entorno.")
            return env_url, "entorno", None

        try:
            doc = self.db.collection("ServidorIA").document("url_actual").get()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Validación IA: error al consultar Firestore para ServidorIA/url_actual.")
            return "", "firestore_error", f"Firestore inaccesible: {exc}"

        if not doc.exists:
            LOGGER.warning("Validación IA: no existe documento ServidorIA/url_actual.")
            return "", "firestore_missing_doc", "No existe documento ServidorIA/url_actual."

        data = doc.to_dict() or {}
        firestore_url = str(data.get("url", "") or "").strip().rstrip("/")
        if not firestore_url:
            LOGGER.warning("Validación IA: campo 'url' vacío en ServidorIA/url_actual.")
            return "", "firestore_missing_url", "Campo 'url' vacío en ServidorIA/url_actual."

        LOGGER.info("Validación IA: URL de servicio resuelta desde Firestore (ServidorIA/url_actual).")
        return firestore_url, "firestore", None


class CalibresIAHistoryWindow(tk.Toplevel):
    """Ventana de consulta histórica IA vs calibrador."""

    def __init__(self, parent: tk.Widget, history_repo: CalibresIAHistoryRepository) -> None:
        super().__init__(parent)
        self.title("Histórico IA de calibres")
        self.geometry("1280x700")
        self.minsize(1100, 560)
        self.transient(parent.winfo_toplevel())
        self.history_repo = history_repo
        self._current_rows: dict[str, int] = {}
        self._current_rows_data: list[dict[str, Any]] = []
        self._ultimo_texto_sesgo: str = "Sin registros para calcular sesgo por calibre."

        self.boleta_var = tk.StringVar()
        self.albaran_var = tk.StringVar()
        self.variedad_var = tk.StringVar()
        self.cultivo_var = tk.StringVar()
        self.calidad_var = tk.StringVar(value="TODAS")
        self.prompt_version_var = tk.StringVar(value="TODAS")
        self.estado_var = tk.StringVar(value=f"Origen DB: {self.history_repo.db_path}")
        self.resumen_var = tk.StringVar(
            value=(
                "Registros=0 | Error medio global=0.00 | Error total medio=0.00 | "
                "Dominante IA frecuente=- | Dominante real frecuente=- | "
                "BUENA=0 | ACEPTABLE=0 | MALA=0 | MUY_MALA=0"
            )
        )
        self.resumen_sesgo_var = tk.StringVar(
            value="Sesgo IA por calibre: sin datos (aplique filtros y ejecute búsqueda)."
        )
        self.resumen_version_var = tk.StringVar(value="Comparativa por versión: sin datos.")
        self._ultimo_texto_recomendacion_version = "Comparativa por versión: sin datos."

        self._build_ui()
        self._buscar_historico()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=10)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)
        container.columnconfigure(0, weight=1)

        filtros = ttk.LabelFrame(container, text="Filtros", padding=8)
        filtros.grid(row=0, column=0, sticky="ew")
        for col in range(12):
            filtros.columnconfigure(col, weight=1 if col in {1, 3, 5, 7, 9, 11} else 0)

        ttk.Label(filtros, text="Boleta:").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(filtros, textvariable=self.boleta_var).grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=2)
        ttk.Label(filtros, text="Albarán:").grid(row=0, column=2, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(filtros, textvariable=self.albaran_var).grid(row=0, column=3, sticky="ew", padx=(0, 10), pady=2)
        ttk.Label(filtros, text="Variedad:").grid(row=0, column=4, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(filtros, textvariable=self.variedad_var).grid(row=0, column=5, sticky="ew", padx=(0, 10), pady=2)
        ttk.Label(filtros, text="Cultivo:").grid(row=0, column=6, sticky="w", padx=(0, 6), pady=2)
        ttk.Entry(filtros, textvariable=self.cultivo_var).grid(row=0, column=7, sticky="ew", padx=(0, 10), pady=2)
        ttk.Label(filtros, text="Calidad:").grid(row=0, column=8, sticky="w", padx=(0, 6), pady=2)
        ttk.Combobox(
            filtros,
            textvariable=self.calidad_var,
            state="readonly",
            values=("TODAS", "BUENA", "ACEPTABLE", "MALA", "MUY_MALA"),
            width=14,
        ).grid(row=0, column=9, sticky="ew", padx=(0, 10), pady=2)
        ttk.Label(filtros, text="Prompt ver.:").grid(row=0, column=10, sticky="w", padx=(0, 6), pady=2)
        self.combo_prompt_version = ttk.Combobox(
            filtros,
            textvariable=self.prompt_version_var,
            state="readonly",
            values=("TODAS", "sin_version"),
            width=18,
        )
        self.combo_prompt_version.grid(row=0, column=11, sticky="ew", padx=(0, 10), pady=2)
        ttk.Button(filtros, text="Buscar histórico", command=self._buscar_historico).grid(
            row=1, column=11, sticky="e", pady=(8, 0)
        )

        ttk.Label(container, textvariable=self.estado_var, foreground="#34495e").grid(row=1, column=0, sticky="ew", pady=(8, 3))
        ttk.Label(container, textvariable=self.resumen_var, foreground="#1f618d", wraplength=1180).grid(
            row=2, column=0, sticky="ew", pady=(0, 6)
        )

        frame_tabla = ttk.LabelFrame(container, text="Comparaciones históricas", padding=6)
        frame_tabla.grid(row=3, column=0, sticky="nsew")
        frame_tabla.rowconfigure(0, weight=1)
        frame_tabla.columnconfigure(0, weight=1)

        columns = (
            "fecha_registro",
            "boleta",
            "albaran",
            "variedad",
            "id_foto",
            "modelo_ia",
            "prompt_version",
            "prompt_source",
            "confianza_ia",
            "calibre_dominante_ia",
            "calibre_dominante_real",
            "error_absoluto_medio",
            "error_total_absoluto",
            "calidad",
        )
        self.tree_historial = ttk.Treeview(frame_tabla, columns=columns, show="headings")
        headers = {
            "fecha_registro": "Fecha registro",
            "boleta": "Boleta",
            "albaran": "Albarán",
            "variedad": "Variedad",
            "id_foto": "Id foto",
            "modelo_ia": "Modelo IA",
            "prompt_version": "Prompt ver.",
            "prompt_source": "Prompt source",
            "confianza_ia": "Confianza",
            "calibre_dominante_ia": "Dominante IA",
            "calibre_dominante_real": "Dominante real",
            "error_absoluto_medio": "Error abs. medio",
            "error_total_absoluto": "Error total abs.",
            "calidad": "Calidad",
        }
        widths = {
            "fecha_registro": 160,
            "boleta": 90,
            "albaran": 110,
            "variedad": 120,
            "id_foto": 190,
            "modelo_ia": 150,
            "prompt_version": 140,
            "prompt_source": 140,
            "confianza_ia": 85,
            "calibre_dominante_ia": 120,
            "calibre_dominante_real": 120,
            "error_absoluto_medio": 120,
            "error_total_absoluto": 120,
            "calidad": 105,
        }
        for col in columns:
            self.tree_historial.heading(col, text=headers[col])
            self.tree_historial.column(col, width=widths[col], anchor="w")
        self.tree_historial.grid(row=0, column=0, sticky="nsew")
        self.tree_historial.bind("<Double-1>", self._on_double_click_historial)

        scroll_y = ttk.Scrollbar(frame_tabla, orient="vertical", command=self.tree_historial.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(frame_tabla, orient="horizontal", command=self.tree_historial.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        self.tree_historial.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        frame_sesgo = ttk.LabelFrame(container, text="Análisis de sesgo por calibre", padding=6)
        frame_sesgo.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        frame_sesgo.columnconfigure(0, weight=1)

        sesgo_cols = ("calibre", "media_ia", "media_real", "sesgo", "interpretacion")
        self.tree_sesgo = ttk.Treeview(frame_sesgo, columns=sesgo_cols, show="headings", height=7)
        labels_sesgo = {
            "calibre": "Calibre",
            "media_ia": "Media IA %",
            "media_real": "Media real normalizado %",
            "sesgo": "Sesgo medio IA-Real",
            "interpretacion": "Interpretación",
        }
        widths_sesgo = {
            "calibre": 85,
            "media_ia": 140,
            "media_real": 190,
            "sesgo": 160,
            "interpretacion": 180,
        }
        for col in sesgo_cols:
            self.tree_sesgo.heading(col, text=labels_sesgo[col])
            self.tree_sesgo.column(col, width=widths_sesgo[col], anchor="w")
        self.tree_sesgo.grid(row=0, column=0, sticky="ew")
        ttk.Label(frame_sesgo, textvariable=self.resumen_sesgo_var, foreground="#1f618d", wraplength=1180).grid(
            row=1, column=0, sticky="ew", pady=(6, 4)
        )
        ttk.Button(frame_sesgo, text="Copiar resumen sesgo", command=self._copiar_resumen_sesgo).grid(
            row=2, column=0, sticky="e"
        )

        frame_version = ttk.LabelFrame(container, text="Comparativa por versión", padding=6)
        frame_version.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        frame_version.columnconfigure(0, weight=1)
        version_cols = (
            "prompt_version", "numero_registros", "error_medio_global", "error_total_medio",
            "dominante_ia_mas_frecuente", "dominante_real_mas_frecuente",
            "total_buena", "total_aceptable", "total_mala", "total_muy_mala", "puntuacion",
        )
        self.tree_version = ttk.Treeview(frame_version, columns=version_cols, show="headings", height=5)
        labels = {
            "prompt_version": "Versión",
            "numero_registros": "Registros",
            "error_medio_global": "Error medio",
            "error_total_medio": "Error total medio",
            "dominante_ia_mas_frecuente": "Dom. IA",
            "dominante_real_mas_frecuente": "Dom. real",
            "total_buena": "BUENA",
            "total_aceptable": "ACEPTABLE",
            "total_mala": "MALA",
            "total_muy_mala": "MUY_MALA",
            "puntuacion": "Puntuación",
        }
        for col in version_cols:
            self.tree_version.heading(col, text=labels[col])
            self.tree_version.column(col, width=120, anchor="w")
        self.tree_version.grid(row=0, column=0, sticky="ew")
        ttk.Label(frame_version, textvariable=self.resumen_version_var, foreground="#1f618d", wraplength=1180).grid(
            row=1, column=0, sticky="ew", pady=(6, 4)
        )
        ttk.Button(frame_version, text="Copiar recomendación", command=self._copiar_recomendacion_version).grid(
            row=2, column=0, sticky="e"
        )

    def _buscar_historico(self) -> None:
        self.estado_var.set("Consultando histórico IA...")
        for item in self.tree_historial.get_children():
            self.tree_historial.delete(item)
        self._current_rows.clear()
        self._current_rows_data = []
        self._limpiar_panel_sesgo()

        filtros = {
            "boleta": self.boleta_var.get().strip(),
            "albaran": self.albaran_var.get().strip(),
            "variedad": self.variedad_var.get().strip(),
            "cultivo": self.cultivo_var.get().strip(),
            "calidad": "" if self.calidad_var.get() == "TODAS" else self.calidad_var.get().strip(),
            "prompt_version": self.prompt_version_var.get().strip(),
        }

        def worker() -> None:
            try:
                rows = self.history_repo.list_comparisons_for_bias(**filtros)
                summary = self.history_repo.get_summary(**filtros)
                summary_by_version = self.history_repo.get_summary_by_version(**filtros)
                versions = self.history_repo.list_prompt_versions()
                self.after(0, lambda: self._on_busqueda_ok(rows, summary, summary_by_version, versions))
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: self._on_busqueda_error(error))

        threading.Thread(target=worker, daemon=True).start()

    def _on_busqueda_ok(
        self,
        rows: list[dict[str, Any]],
        summary: dict[str, Any],
        summary_by_version: list[dict[str, Any]],
        versions: list[str],
    ) -> None:
        opciones = ["TODAS", "sin_version", *[v for v in versions if v != "sin_version"]]
        self.combo_prompt_version.configure(values=opciones)
        if self.prompt_version_var.get() not in opciones:
            self.prompt_version_var.set("TODAS")
        if not rows:
            self.estado_var.set("Sin registros para los filtros aplicados.")
        else:
            self.estado_var.set(f"Consulta OK. Registros cargados: {len(rows)}.")
        for row in rows:
            item_id = f"hist_{row.get('id')}"
            self._current_rows[item_id] = int(row.get("id") or 0)
            self.tree_historial.insert(
                "",
                "end",
                iid=item_id,
                values=(
                    row.get("fecha_registro", "-"),
                    row.get("boleta", "-"),
                    row.get("albaran", "-"),
                    row.get("variedad", "-"),
                    row.get("id_foto", "-"),
                    row.get("modelo_ia", "-"),
                    row.get("prompt_version", "sin_version"),
                    row.get("prompt_source", "no_informado"),
                    self._fmt_number(row.get("confianza_ia")),
                    row.get("calibre_dominante_ia", "-"),
                    row.get("calibre_dominante_real", "-"),
                    self._fmt_number(row.get("error_absoluto_medio")),
                    self._fmt_number(row.get("error_total_absoluto")),
                    row.get("calidad", clasificar_calidad_error(row.get("error_absoluto_medio"))),
                ),
            )
        self.resumen_var.set(
            "Registros={numero_registros} | Error medio global={error_medio_global} | "
            "Error total medio={error_total_medio} | Dominante IA frecuente={dominante_ia_mas_frecuente} | "
            "Dominante real frecuente={dominante_real_mas_frecuente} | BUENA={total_buena} | "
            "ACEPTABLE={total_aceptable} | MALA={total_mala} | MUY_MALA={total_muy_mala}".format(
                numero_registros=summary.get("numero_registros", 0),
                error_medio_global=self._fmt_number(summary.get("error_medio_global")),
                error_total_medio=self._fmt_number(summary.get("error_total_medio")),
                dominante_ia_mas_frecuente=summary.get("dominante_ia_mas_frecuente", "-"),
                dominante_real_mas_frecuente=summary.get("dominante_real_mas_frecuente", "-"),
                total_buena=summary.get("total_buena", 0),
                total_aceptable=summary.get("total_aceptable", 0),
                total_mala=summary.get("total_mala", 0),
                total_muy_mala=summary.get("total_muy_mala", 0),
            )
        )
        analisis_sesgo = calcular_sesgo_por_calibre(rows)
        self._current_rows_data = rows
        self._render_sesgo(analisis_sesgo)
        self._render_summary_version(summary_by_version)

    def _on_busqueda_error(self, exc: Exception) -> None:
        self.estado_var.set(f"Error consultando histórico IA: {exc}")
        self.resumen_var.set(
            "Registros=0 | Error medio global=- | Error total medio=- | Dominante IA frecuente=- | Dominante real frecuente=- | "
            "BUENA=0 | ACEPTABLE=0 | MALA=0 | MUY_MALA=0"
        )
        self._limpiar_panel_sesgo()
        self._render_summary_version([])
        messagebox.showerror("Histórico IA", f"No se pudo consultar la base histórica:\n{exc}", parent=self)

    def _render_summary_version(self, summary_by_version: list[dict[str, Any]]) -> None:
        for item in self.tree_version.get_children():
            self.tree_version.delete(item)
        if not summary_by_version:
            mensaje = "Comparativa por versión: sin datos."
            self.resumen_version_var.set(mensaje)
            self._ultimo_texto_recomendacion_version = mensaje
            return
        filas_puntuadas = []
        for item in summary_by_version:
            puntuacion = calcular_puntuacion_version(item)
            item_con_puntuacion = {**item, "puntuacion": puntuacion}
            filas_puntuadas.append(item_con_puntuacion)
            self.tree_version.insert(
                "",
                "end",
                values=(
                    item_con_puntuacion.get("prompt_version", "sin_version"),
                    item_con_puntuacion.get("numero_registros", 0),
                    self._fmt_number(item_con_puntuacion.get("error_medio_global")),
                    self._fmt_number(item_con_puntuacion.get("error_total_medio")),
                    item_con_puntuacion.get("dominante_ia_mas_frecuente", "-"),
                    item_con_puntuacion.get("dominante_real_mas_frecuente", "-"),
                    item_con_puntuacion.get("total_buena", 0),
                    item_con_puntuacion.get("total_aceptable", 0),
                    item_con_puntuacion.get("total_mala", 0),
                    item_con_puntuacion.get("total_muy_mala", 0),
                    self._fmt_number(item_con_puntuacion.get("puntuacion")),
                ),
            )
        recomendacion = generar_recomendacion_versiones(filas_puntuadas)
        self.resumen_version_var.set(recomendacion.get("mensaje", "Comparativa por versión: sin datos."))
        self._ultimo_texto_recomendacion_version = recomendacion.get("copiable", "Comparativa por versión: sin datos.")

    def _render_sesgo(self, analisis: dict[str, Any]) -> None:
        for item in self.tree_sesgo.get_children():
            self.tree_sesgo.delete(item)
        for fila in analisis.get("filas", []):
            self.tree_sesgo.insert(
                "",
                "end",
                values=(
                    fila.get("calibre", "-"),
                    self._fmt_number(fila.get("media_ia")),
                    self._fmt_number(fila.get("media_real")),
                    self._fmt_number(fila.get("sesgo")),
                    fila.get("interpretacion", "-"),
                ),
            )
        resumen = analisis.get("resumen", {})
        if int(resumen.get("total_registros", 0)) <= 0:
            self.resumen_sesgo_var.set("Sesgo IA por calibre: sin registros para los filtros aplicados.")
        else:
            self.resumen_sesgo_var.set(
                "Más sobreestimado={calibre_max} ({sesgo_max}) | Más infraestimado={calibre_min} ({sesgo_min}) | "
                "Tendencia general={tendencia}".format(
                    calibre_max=resumen.get("calibre_mas_sobreestimado", "-"),
                    sesgo_max=self._fmt_number(resumen.get("sesgo_max")),
                    calibre_min=resumen.get("calibre_mas_infraestimado", "-"),
                    sesgo_min=self._fmt_number(resumen.get("sesgo_min")),
                    tendencia=resumen.get("tendencia_general", "Sin datos"),
                )
            )
        self._ultimo_texto_sesgo = str(analisis.get("texto_resumen", "") or "Sin registros para calcular sesgo por calibre.")

    def _limpiar_panel_sesgo(self) -> None:
        self._render_sesgo(calcular_sesgo_por_calibre([]))

    def _copiar_resumen_sesgo(self) -> None:
        texto = self._ultimo_texto_sesgo.strip() or "Sin registros para calcular sesgo por calibre."
        try:
            self.clipboard_clear()
            self.clipboard_append(texto)
            self.estado_var.set("Resumen de sesgo copiado al portapapeles.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Histórico IA", f"No se pudo copiar el resumen de sesgo:\n{exc}", parent=self)

    def _copiar_recomendacion_version(self) -> None:
        texto = self._ultimo_texto_recomendacion_version.strip() or "Comparativa por versión: sin datos."
        try:
            self.clipboard_clear()
            self.clipboard_append(texto)
            self.estado_var.set("Recomendación por versión copiada al portapapeles.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Histórico IA", f"No se pudo copiar la recomendación:\n{exc}", parent=self)

    @staticmethod
    def _fmt_number(value: Any) -> str:
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return "-"

    def _on_double_click_historial(self, _event: tk.Event) -> None:
        selected = self.tree_historial.selection()
        if not selected:
            return
        comparison_id = self._current_rows.get(selected[0], 0)
        if comparison_id <= 0:
            return

        def worker() -> None:
            try:
                detail = self.history_repo.get_comparison_detail(comparison_id)
                if not detail:
                    raise LookupError(f"No se encontró detalle para id={comparison_id}.")
                self.after(0, lambda: self._open_detail_window(detail))
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: messagebox.showerror("Histórico IA", str(error), parent=self))

        threading.Thread(target=worker, daemon=True).start()

    def _open_detail_window(self, detail: dict[str, Any]) -> None:
        win = tk.Toplevel(self)
        win.title(f"Detalle histórico IA | id={detail.get('id')}")
        win.geometry("980x680")
        win.minsize(840, 520)

        frame = ttk.Frame(win, padding=10)
        frame.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)
        frame.columnconfigure(0, weight=1)

        meta = (
            f"Fecha={detail.get('fecha_registro', '-')} | Boleta={detail.get('boleta', '-')} | "
            f"Albarán={detail.get('albaran', '-')} | Variedad={detail.get('variedad', '-')} | "
            f"Cultivo={detail.get('cultivo', '-') or '-'} | "
            f"Prompt={str(detail.get('prompt_version', '') or '').strip() or 'sin_version'} | "
            f"Prompt source={str(detail.get('prompt_source', '') or '').strip() or 'no_informado'} | "
            f"Foto={detail.get('id_foto', '-')}"
        )
        ttk.Label(frame, text=meta, foreground="#34495e", wraplength=940).grid(row=0, column=0, sticky="w", pady=(0, 8))

        text = tk.Text(frame, wrap="none")
        text.grid(row=1, column=0, sticky="nsew")
        scroll_y = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        scroll_y.grid(row=1, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(frame, orient="horizontal", command=text.xview)
        scroll_x.grid(row=2, column=0, sticky="ew")
        text.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        lineas = ["=== IA CAL0..CAL9 ==="]
        for idx in range(10):
            lineas.append(f"IA CAL{idx}: {self._fmt_number(detail.get(f'ia_cal{idx}'))}%")
        lineas.append("")
        lineas.append("=== Real normalizado CAL0..CAL9 ===")
        for idx in range(10):
            lineas.append(f"Real norm CAL{idx}: {self._fmt_number(detail.get(f'real_norm_cal{idx}'))}%")
        lineas.append("")
        lineas.append("=== Real bruto CAL0..CAL9 ===")
        for idx in range(10):
            lineas.append(f"Real bruto CAL{idx}: {self._fmt_number(detail.get(f'real_bruto_cal{idx}'))}%")
        lineas.extend(
            [
                "",
                "=== Calidad comparación ===",
                f"Calidad: {clasificar_calidad_error(detail.get('error_absoluto_medio'))}",
                "",
                "=== Metadatos IA ===",
                f"modelo_ia: {detail.get('modelo_ia', '-')}",
                f"prompt_version: {str(detail.get('prompt_version', '') or '').strip() or 'sin_version'}",
                f"prompt_source: {str(detail.get('prompt_source', '') or '').strip() or 'no_informado'}",
                f"cultivo: {str(detail.get('cultivo', '') or '').strip() or '-'}",
                f"variedad: {str(detail.get('variedad', '') or '').strip() or '-'}",
                "",
                f"podrido: {self._fmt_number(detail.get('podrido'))}",
                f"deslinea: {self._fmt_number(detail.get('deslinea'))}",
                f"desmesa: {self._fmt_number(detail.get('desmesa'))}",
                f"destrio_total: {self._fmt_number(detail.get('destrio_total'))}",
                "",
                f"advertencias_ia: {detail.get('advertencias_ia', '-')}",
                "",
                "=== resumen_ia ===",
                str(detail.get("resumen_ia", "")),
                "",
                "=== output_ia_json ===",
                str(detail.get("output_ia_json", "")),
            ]
        )
        text.insert("1.0", "\n".join(lineas))
        text.configure(state="disabled")


class ObtencionCalibresWindow(BaseToolWindow):
    """UI para flujo boleta -> muestras -> fotos de pantalla de calibres."""

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent, db_firestore)
        self.title("Obtención calibres")
        self.geometry("1220x720")
        self.minsize(1060, 620)

        self.config_repo = CalibresConfigRepository(db_firestore)
        self.data_service = CalibresDataService(db_firestore)
        self.history_repo = CalibresIAHistoryRepository()

        self.boleta_var = tk.StringVar()
        self.estado_var = tk.StringVar(value="Ingrese una boleta para comenzar.")
        self.pantalla_var = tk.StringVar(value="Datos Calibres")
        self.diametro_var = tk.StringVar(value="94.0")

        self._config: CalibresConfig | None = None
        self._muestras: list[dict[str, Any]] = []
        self._fotos_by_muestra: dict[str, list[dict[str, Any]]] = {}
        self._selected_fotos_by_muestra: dict[str, set[str]] = {}
        self._current_muestra_id: str | None = None
        self._current_cards: list[dict[str, Any]] = []
        self._preview_refs: list[Any] = []
        self._fullsize_refs: list[Any] = []
        self._analysis_payload: dict[str, Any] = {}
        self._deteccion_resultados: dict[str, CircleDetectionResult] = {}
        self._detector: CirclePatternDetector | None = None
        self._overlay_paths_by_foto: dict[str, str] = {}
        self._frutos_resultados: dict[str, PhotoFruitAnalysisResult] = {}
        self._frutos_overlay_paths_by_foto: dict[str, str] = {}
        self._ia_validacion_resultados_by_muestra: dict[str, dict[str, dict[str, Any]]] = {}
        self._ia_estimacion_resultados_by_muestra: dict[str, dict[str, dict[str, Any]]] = {}
        self._overlay_dir = Path(tempfile.gettempdir()) / "harvestsync_desk" / "calibres_overlays"
        self._overlay_dir.mkdir(parents=True, exist_ok=True)
        self._fruit_analyzer = FruitCaliberAnalyzer()
        self._ai_validacion_en_curso = False
        self._ai_lote_en_curso = False
        self._ai_estimacion_en_curso = False
        self._flujo_recomendado_en_curso = False
        self._comparacion_ia_vs_calibrador: dict[str, Any] = {}
        self._contexto_comparacion_actual: dict[str, Any] = {}
        self._entregas_calibrador: list[dict[str, Any]] = []
        self._boleta_entregas_calibrador: str = ""
        self._selector_entrega_map: dict[str, dict[str, Any] | None] = {}
        self.selector_entrega_var = tk.StringVar(value=OPCION_BOLETA_COMPLETA)
        self._history_window: CalibresIAHistoryWindow | None = None

        self._build_ui()
        self._cargar_configuracion()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)

        filtros = ttk.LabelFrame(container, text="Cabecera operativa", padding=10)
        filtros.grid(row=0, column=0, sticky="ew")
        filtros.columnconfigure(1, weight=1)

        ttk.Label(filtros, text="Boleta:").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        entry_boleta = ttk.Entry(filtros, textvariable=self.boleta_var)
        entry_boleta.grid(row=0, column=1, sticky="ew", pady=4)
        entry_boleta.bind("<Return>", lambda _: self._buscar_boleta())

        ttk.Button(filtros, text="🔍 Buscar", command=self._buscar_boleta).grid(row=0, column=2, padx=(8, 0), pady=4)
        ttk.Button(filtros, text="↺ Recargar config", command=self._cargar_configuracion).grid(row=0, column=3, padx=(8, 0), pady=4)

        ttk.Label(filtros, text="Pantalla objetivo:").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Label(filtros, textvariable=self.pantalla_var).grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(filtros, text="Diámetro patrón (mm):").grid(row=1, column=2, sticky="e", padx=(12, 6), pady=4)
        ttk.Label(filtros, textvariable=self.diametro_var).grid(row=1, column=3, sticky="w", pady=4)

        ttk.Label(container, textvariable=self.estado_var, foreground="#34495e").grid(row=1, column=0, sticky="ew", pady=(8, 8))

        notebook = ttk.Notebook(container)
        notebook.grid(row=2, column=0, sticky="nsew")

        tab_muestras_fotos = ttk.Frame(notebook, padding=8)
        tab_validacion_ia = ttk.Frame(notebook, padding=8)
        tab_patron_escala = ttk.Frame(notebook, padding=8)
        tab_frutos_calibres = ttk.Frame(notebook, padding=8)
        tab_resumen = ttk.Frame(notebook, padding=8)

        notebook.add(tab_muestras_fotos, text="Muestras y fotos")
        notebook.add(tab_validacion_ia, text="Validación IA")
        notebook.add(tab_patron_escala, text="Patrón y escala")
        notebook.add(tab_frutos_calibres, text="Frutos y calibres")
        notebook.add(tab_resumen, text="Resumen")

        tab_muestras_fotos.rowconfigure(1, weight=1)
        tab_muestras_fotos.columnconfigure(0, weight=1)
        tab_muestras_fotos.columnconfigure(1, weight=2)

        frame_muestras = ttk.LabelFrame(tab_muestras_fotos, text="Muestras asociadas", padding=8)
        frame_muestras.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        frame_muestras.rowconfigure(0, weight=1)
        frame_muestras.columnconfigure(0, weight=1)

        self.tree_muestras = ttk.Treeview(
            frame_muestras,
            columns=("id_muestra", "nombre", "cultivo", "fecha_hora", "fotos"),
            show="headings",
            selectmode="browse",
        )
        headers = {
            "id_muestra": "Id muestra",
            "nombre": "Nombre",
            "cultivo": "Cultivo",
            "fecha_hora": "FechaHora",
            "fotos": "Fotos calibres",
        }
        widths = {
            "id_muestra": 180,
            "nombre": 150,
            "cultivo": 110,
            "fecha_hora": 150,
            "fotos": 110,
        }
        for col in headers:
            self.tree_muestras.heading(col, text=headers[col])
            self.tree_muestras.column(col, width=widths[col], anchor="w")
        self.tree_muestras.grid(row=0, column=0, sticky="nsew")
        scroll_muestras = ttk.Scrollbar(frame_muestras, orient="vertical", command=self.tree_muestras.yview)
        scroll_muestras.grid(row=0, column=1, sticky="ns")
        self.tree_muestras.configure(yscrollcommand=scroll_muestras.set)
        self.tree_muestras.bind("<<TreeviewSelect>>", self._on_select_muestra)

        frame_fotos = ttk.LabelFrame(tab_muestras_fotos, text="Fotos de 'Datos Calibres'", padding=8)
        frame_fotos.grid(row=1, column=1, sticky="nsew")
        frame_fotos.rowconfigure(2, weight=1)
        frame_fotos.columnconfigure(0, weight=1)

        toolbar = ttk.Frame(tab_muestras_fotos)
        toolbar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        toolbar.columnconfigure(8, weight=1)
        self.btn_seleccionar_todas = ttk.Button(toolbar, text="Seleccionar todas", command=self._seleccionar_todas)
        self.btn_seleccionar_todas.grid(row=0, column=0, padx=(0, 6))
        self.btn_deseleccionar_todas = ttk.Button(toolbar, text="Deseleccionar todas", command=self._deseleccionar_todas)
        self.btn_deseleccionar_todas.grid(row=0, column=1, padx=(0, 6))
        self.btn_invertir_seleccion = ttk.Button(toolbar, text="Invertir selección", command=self._invertir_seleccion)
        self.btn_invertir_seleccion.grid(row=0, column=2, padx=(0, 6))

        self.resumen_fotos_var = tk.StringVar(value="Fotos encontradas: 0 | Seleccionadas: 0 | Excluidas: 0")
        ttk.Label(frame_fotos, textvariable=self.resumen_fotos_var, foreground="#34495e").grid(row=1, column=0, sticky="w", pady=(0, 6))

        self.canvas_fotos = tk.Canvas(frame_fotos, highlightthickness=0)
        self.canvas_fotos.grid(row=2, column=0, sticky="nsew")
        self.scroll_fotos = ttk.Scrollbar(frame_fotos, orient="vertical", command=self.canvas_fotos.yview)
        self.scroll_fotos.grid(row=2, column=1, sticky="ns")
        self.canvas_fotos.configure(yscrollcommand=self.scroll_fotos.set)

        self.frame_fotos_content = ttk.Frame(self.canvas_fotos)
        self._fotos_window = self.canvas_fotos.create_window((0, 0), window=self.frame_fotos_content, anchor="nw")

        self.frame_fotos_content.bind("<Configure>", lambda _: self.canvas_fotos.configure(scrollregion=self.canvas_fotos.bbox("all")))
        self.canvas_fotos.bind("<Configure>", self._sync_fotos_width)

        tab_validacion_ia.rowconfigure(2, weight=1)
        tab_validacion_ia.rowconfigure(3, weight=2)
        tab_validacion_ia.columnconfigure(0, weight=1)
        toolbar_ia = ttk.Frame(tab_validacion_ia)
        toolbar_ia.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar_ia.columnconfigure(7, weight=1)
        self.btn_validacion_ia = ttk.Button(toolbar_ia, text="🤖 Validación IA", command=self._ejecutar_validacion_ia)
        self.btn_validacion_ia.grid(row=0, column=0, padx=(0, 6))
        self.btn_validar_lote_ia = ttk.Button(toolbar_ia, text="🤖 Validar lote IA", command=self._validar_lote_ia)
        self.btn_validar_lote_ia.grid(row=0, column=1, padx=(0, 6))
        self.btn_usar_solo_aptas_ia = ttk.Button(toolbar_ia, text="✅ Usar solo aptas IA", command=self._usar_solo_aptas_ia)
        self.btn_usar_solo_aptas_ia.grid(row=0, column=2, padx=(0, 6))
        self.btn_estimacion_calibres_ia = ttk.Button(
            toolbar_ia,
            text="🧪 Estimación calibres IA",
            command=self._ejecutar_estimacion_calibres_ia,
        )
        self.btn_estimacion_calibres_ia.grid(row=0, column=3, padx=(0, 6))
        self.btn_ver_detalle_estimacion_ia = ttk.Button(
            toolbar_ia,
            text="📄 Ver detalle IA",
            command=self._ver_detalle_estimacion_ia_seleccionada,
        )
        self.btn_ver_detalle_estimacion_ia.grid(row=0, column=4, padx=(0, 6))
        self.btn_comparar_calibrador = ttk.Button(
            toolbar_ia,
            text="📊 Comparar IA vs calibrador",
            command=self._comparar_ia_vs_calibrador,
        )
        self.btn_comparar_calibrador.grid(row=0, column=5, padx=(0, 6))
        self.btn_guardar_historico = ttk.Button(
            toolbar_ia,
            text="💾 Guardar comparación histórico",
            command=self._guardar_comparacion_historico,
        )
        self.btn_guardar_historico.grid(row=0, column=6, padx=(0, 6))
        self.btn_ver_historico_ia = ttk.Button(
            toolbar_ia,
            text="📚 Ver histórico IA",
            command=self._abrir_panel_historico_ia,
        )
        self.btn_ver_historico_ia.grid(row=0, column=7, padx=(0, 6))

        ia_frame = ttk.LabelFrame(tab_validacion_ia, text="Resultados IA por foto", padding=6)
        ia_frame.grid(row=1, column=0, sticky="nsew")
        ia_frame.rowconfigure(0, weight=1)
        ia_frame.columnconfigure(0, weight=1)

        self.tree_validacion_ia = ttk.Treeview(
            ia_frame,
            columns=("id_foto", "apta", "confianza", "oclusion", "patron_visible", "estado"),
            show="headings",
            height=6,
        )
        headers_ia = {
            "id_foto": "Foto",
            "apta": "IA apta",
            "confianza": "Confianza",
            "oclusion": "Oclusión",
            "patron_visible": "Patrón visible",
            "estado": "Estado/Error",
        }
        widths_ia = {"id_foto": 180, "apta": 80, "confianza": 85, "oclusion": 90, "patron_visible": 120, "estado": 420}
        for col in headers_ia:
            self.tree_validacion_ia.heading(col, text=headers_ia[col])
            self.tree_validacion_ia.column(col, width=widths_ia[col], anchor="w")
        self.tree_validacion_ia.grid(row=0, column=0, sticky="nsew")
        self.resumen_ia_lote_var = tk.StringVar(
            value="IA lote: evaluadas=0 | aptas=0 | no aptas=0 | errores=0 | confianza media=-"
        )
        ttk.Label(ia_frame, textvariable=self.resumen_ia_lote_var, foreground="#34495e").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )

        estimacion_frame = ttk.LabelFrame(tab_validacion_ia, text="Estimación IA experimental por foto", padding=6)
        estimacion_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        estimacion_frame.rowconfigure(0, weight=1)
        estimacion_frame.columnconfigure(0, weight=1)

        self.tree_estimacion_ia = ttk.Treeview(
            estimacion_frame,
            columns=("id_foto", "apta", "confianza", "frutos", "dominante", "distribucion", "estado"),
            show="headings",
            height=6,
        )
        headers_estimacion = {
            "id_foto": "Foto",
            "apta": "Apta estimación",
            "confianza": "Confianza",
            "frutos": "Frutos visibles",
            "dominante": "Calibre dominante",
            "distribucion": "Distribución",
            "estado": "Estado/Error",
        }
        widths_estimacion = {
            "id_foto": 170,
            "apta": 110,
            "confianza": 80,
            "frutos": 95,
            "dominante": 120,
            "distribucion": 260,
            "estado": 260,
        }
        for col in headers_estimacion:
            self.tree_estimacion_ia.heading(col, text=headers_estimacion[col])
            self.tree_estimacion_ia.column(col, width=widths_estimacion[col], anchor="w")
        self.tree_estimacion_ia.grid(row=0, column=0, sticky="nsew")
        self.tree_estimacion_ia.bind("<Double-1>", self._on_double_click_estimacion_ia)
        self.resumen_estimacion_ia_var = tk.StringVar(
            value="Estimación IA experimental: evaluadas=0 | aptas=0 | confianza media=- | distribución consolidada=-"
        )
        ttk.Label(estimacion_frame, textvariable=self.resumen_estimacion_ia_var, foreground="#34495e").grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )
        self.advertencias_estimacion_ia_var = tk.StringVar(value="Advertencias estimación IA experimental: -")
        ttk.Label(
            estimacion_frame,
            textvariable=self.advertencias_estimacion_ia_var,
            foreground="#7d6608",
            wraplength=980,
            justify="left",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))

        comparacion_frame = ttk.LabelFrame(tab_validacion_ia, text="Comparación IA vs calibrador normalizado", padding=6)
        comparacion_frame.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        comparacion_frame.rowconfigure(2, weight=1)
        comparacion_frame.rowconfigure(4, weight=1)
        comparacion_frame.columnconfigure(0, weight=1)

        selector_frame = ttk.Frame(comparacion_frame)
        selector_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        selector_frame.columnconfigure(1, weight=1)
        ttk.Button(selector_frame, text="📥 Cargar entregas calibrador", command=self._cargar_entregas_calibrador).grid(
            row=0, column=0, padx=(0, 6)
        )
        self.combo_entrega_calibrador = ttk.Combobox(
            selector_frame,
            textvariable=self.selector_entrega_var,
            state="readonly",
            values=[OPCION_BOLETA_COMPLETA],
        )
        self.combo_entrega_calibrador.grid(row=0, column=1, sticky="ew")
        self.combo_entrega_calibrador.bind("<<ComboboxSelected>>", lambda _evt: self._actualizar_texto_contexto_comparacion())

        self.contexto_comparacion_var = tk.StringVar(value="Contexto: Boleta completa ponderada.")
        ttk.Label(selector_frame, textvariable=self.contexto_comparacion_var, foreground="#34495e").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(4, 0)
        )

        self.resumen_calibrador_var = tk.StringVar(
            value="Calibrador total partida: Podrido=- | DesLínea=- | DesMesa=- | Destrío total=- | ΣCAL0..CAL9=-"
        )
        ttk.Label(comparacion_frame, textvariable=self.resumen_calibrador_var, foreground="#34495e").grid(
            row=1, column=0, sticky="w", pady=(0, 6)
        )
        self.tree_comparacion_calibres = ttk.Treeview(
            comparacion_frame,
            columns=("calibre", "ia", "real_norm", "real_bruto", "dif_abs"),
            show="headings",
            height=6,
        )
        headers_comp = {
            "calibre": "Calibre",
            "ia": "% IA",
            "real_norm": "% Real normalizado",
            "real_bruto": "% Real bruto total",
            "dif_abs": "Dif. absoluta",
        }
        widths_comp = {"calibre": 80, "ia": 110, "real_norm": 150, "real_bruto": 140, "dif_abs": 110}
        for col in headers_comp:
            self.tree_comparacion_calibres.heading(col, text=headers_comp[col])
            self.tree_comparacion_calibres.column(col, width=widths_comp[col], anchor="w")
        self.tree_comparacion_calibres.grid(row=2, column=0, sticky="nsew")

        self.resumen_metricas_comparacion_var = tk.StringVar(
            value="Métricas: error absoluto medio=- | error total absoluto=- | dominante IA=- | dominante real normalizado=-"
        )
        ttk.Label(comparacion_frame, textvariable=self.resumen_metricas_comparacion_var, foreground="#34495e").grid(
            row=3, column=0, sticky="w", pady=(6, 2)
        )
        self.nota_comparacion_var = tk.StringVar(
            value="Comparación realizada sobre distribución de calibres normalizada, excluyendo podrido y destrío."
        )
        ttk.Label(
            comparacion_frame,
            textvariable=self.nota_comparacion_var,
            foreground="#7d6608",
            wraplength=980,
            justify="left",
        ).grid(row=4, column=0, sticky="w", pady=(2, 0))

        tab_patron_escala.rowconfigure(1, weight=1)
        tab_patron_escala.columnconfigure(0, weight=1)
        toolbar_patron = ttk.Frame(tab_patron_escala)
        toolbar_patron.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar_patron.columnconfigure(2, weight=1)
        self.btn_detectar_patron = ttk.Button(toolbar_patron, text="🎯 Detectar patrón", command=self._detectar_patron_y_escala)
        self.btn_detectar_patron.grid(row=0, column=0, padx=(0, 6))
        ttk.Button(toolbar_patron, text="👁 Ver validación visual", command=self._abrir_overlay_resultado_actual).grid(
            row=0,
            column=1,
            padx=(0, 6),
        )

        resultados = ttk.LabelFrame(tab_patron_escala, text="Detección patrón y escala", padding=6)
        resultados.grid(row=1, column=0, sticky="nsew")
        resultados.rowconfigure(0, weight=1)
        resultados.columnconfigure(0, weight=1)

        self.tree_resultados = ttk.Treeview(
            resultados,
            columns=("id_foto", "detectado", "diametro_px", "mm_px", "valida", "estado"),
            show="headings",
            height=10,
        )
        headers = {
            "id_foto": "Foto",
            "detectado": "Patrón",
            "diametro_px": "Diámetro (px)",
            "mm_px": "mm/px",
            "valida": "Válida",
            "estado": "Estado",
        }
        widths = {"id_foto": 190, "detectado": 80, "diametro_px": 110, "mm_px": 110, "valida": 70, "estado": 330}
        for col in headers:
            self.tree_resultados.heading(col, text=headers[col])
            self.tree_resultados.column(col, width=widths[col], anchor="w")
        self.tree_resultados.grid(row=0, column=0, sticky="nsew")
        self.tree_resultados.bind("<Double-1>", self._on_double_click_resultado)

        tab_frutos_calibres.rowconfigure(1, weight=1)
        tab_frutos_calibres.columnconfigure(0, weight=1)
        toolbar_frutos = ttk.Frame(tab_frutos_calibres)
        toolbar_frutos.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar_frutos.columnconfigure(2, weight=1)
        self.btn_analizar_frutos = ttk.Button(toolbar_frutos, text="🍊 Analizar frutos", command=self._analizar_frutos)
        self.btn_analizar_frutos.grid(row=0, column=0, padx=(0, 6))
        ttk.Button(toolbar_frutos, text="👁 Ver overlay frutos", command=self._abrir_overlay_frutos_actual).grid(
            row=0,
            column=1,
            padx=(0, 6),
        )

        frutos = ttk.LabelFrame(tab_frutos_calibres, text="Estimación prudente de frutos por calibre", padding=6)
        frutos.grid(row=1, column=0, sticky="nsew")
        frutos.rowconfigure(0, weight=1)
        frutos.columnconfigure(0, weight=1)

        self.tree_frutos_foto = ttk.Treeview(
            frutos,
            columns=("id_foto", "detectados", "validos", "descartados", "descarte_pct", "estado"),
            show="headings",
            height=10,
        )
        headers_frutos = {
            "id_foto": "Foto",
            "detectados": "Detectados",
            "validos": "Válidos",
            "descartados": "Descartados",
            "descarte_pct": "% descarte",
            "estado": "Estado",
        }
        widths_frutos = {"id_foto": 180, "detectados": 90, "validos": 80, "descartados": 95, "descarte_pct": 95, "estado": 340}
        for col in headers_frutos:
            self.tree_frutos_foto.heading(col, text=headers_frutos[col])
            self.tree_frutos_foto.column(col, width=widths_frutos[col], anchor="w")
        self.tree_frutos_foto.grid(row=0, column=0, sticky="nsew")
        self.tree_frutos_foto.bind("<Double-1>", self._abrir_overlay_frutos_actual)

        tab_resumen.columnconfigure(0, weight=1)
        resumen_frame = ttk.LabelFrame(tab_resumen, text="Resumen global del análisis", padding=8)
        resumen_frame.grid(row=0, column=0, sticky="new")
        resumen_frame.columnconfigure(1, weight=1)

        self.resumen_global_var = tk.StringVar(
            value=(
                "Fotos encontradas: 0 | Fotos seleccionadas: 0 | Fotos aptas IA: 0 | "
                "Fotos patrón válido: 0 | Frutos válidos: 0"
            )
        )
        ttk.Label(
            resumen_frame,
            textvariable=self.resumen_global_var,
            foreground="#34495e",
            wraplength=920,
            justify="left",
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))

        ttk.Label(resumen_frame, text="Estado IA lote:", font=("Segoe UI", 9, "bold")).grid(row=1, column=0, sticky="nw", padx=(0, 8))
        ttk.Label(resumen_frame, textvariable=self.resumen_ia_lote_var, wraplength=920, justify="left").grid(row=1, column=1, sticky="w")

        estado_fases_frame = ttk.LabelFrame(tab_resumen, text="Estado operativo por fases", padding=8)
        estado_fases_frame.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        estado_fases_frame.columnconfigure(0, weight=1)
        self.resumen_fases_var = tk.StringVar(
            value=(
                "1. Selección: 0/0 fotos\n"
                "2. IA: pendiente\n"
                "3. Patrón: pendiente\n"
                "4. Frutos: pendiente\n"
                "5. Preparación: pendiente\n"
                "Estado final: pendiente"
            )
        )
        ttk.Label(estado_fases_frame, textvariable=self.resumen_fases_var, justify="left", foreground="#34495e").grid(
            row=0, column=0, sticky="w"
        )

        acciones_resumen = ttk.Frame(tab_resumen)
        acciones_resumen.grid(row=2, column=0, sticky="e", pady=(10, 0))
        self.btn_ejecutar_flujo = ttk.Button(
            acciones_resumen,
            text="▶ Ejecutar flujo recomendado",
            command=self._ejecutar_flujo_recomendado,
        )
        self.btn_ejecutar_flujo.grid(row=0, column=0, padx=(0, 8))
        ttk.Button(acciones_resumen, text="🧮 Preparar análisis", command=self._preparar_analisis).grid(row=0, column=1, sticky="e")

        self.btn_preparar_analisis = ttk.Button(tab_muestras_fotos, text="🧮 Preparar análisis", command=self._preparar_analisis)
        self.btn_preparar_analisis.grid(row=2, column=1, sticky="e", pady=(8, 0))

        self._actualizar_resumen_global()

    def _sync_fotos_width(self, event: tk.Event) -> None:
        self.canvas_fotos.itemconfigure(self._fotos_window, width=event.width)

    def _cargar_configuracion(self) -> None:
        try:
            self._config = self.config_repo.load()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Obtención calibres", f"No se pudo leer configuración: {exc}", parent=self)
            self._config = CalibresConfig(diametro_patron_mm=94.0, pantalla_fotos="Datos Calibres", rangos_por_cultivo={})

        self.pantalla_var.set(self._config.pantalla_fotos)
        self.diametro_var.set(f"{self._config.diametro_patron_mm:.2f}")
        self._detector = CirclePatternDetector(self._config.diametro_patron_mm)

    def _buscar_boleta(self) -> None:
        boleta = self.boleta_var.get().strip()
        if not boleta:
            messagebox.showinfo("Obtención calibres", "Ingrese una boleta.", parent=self)
            return

        self.estado_var.set(f"Buscando boleta {boleta}...")
        self._clear_tree()
        self._limpiar_fotos()
        self._fotos_by_muestra = {}
        self._selected_fotos_by_muestra = {}
        self._current_muestra_id = None
        self._current_cards = []
        self._analysis_payload = {}
        self._deteccion_resultados = {}
        self._overlay_paths_by_foto = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._ia_validacion_resultados_by_muestra = {}
        self._ia_estimacion_resultados_by_muestra = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._limpiar_resultados_ia()
        self._limpiar_resultados_estimacion_ia()
        self._limpiar_resultados_comparacion_calibres()
        self._limpiar_selector_entregas()

        def worker() -> None:
            try:
                muestras = self.data_service.get_muestras_by_boleta(boleta)
                pantalla = self._config.pantalla_fotos if self._config else "Datos Calibres"
                fotos_by_muestra: dict[str, list[dict[str, Any]]] = {}
                for muestra in muestras:
                    id_muestra = muestra["id_muestra"]
                    fotos_by_muestra[id_muestra] = self.data_service.get_fotos_by_muestra(id_muestra, pantalla)
                self.after(0, lambda: self._on_busqueda_ok(boleta, muestras, fotos_by_muestra))
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: self._on_busqueda_error(error))

        threading.Thread(target=worker, daemon=True).start()

    def _on_busqueda_ok(self, boleta: str, muestras: list[dict[str, Any]], fotos_by_muestra: dict[str, list[dict[str, Any]]]) -> None:
        self._muestras = muestras
        self._fotos_by_muestra = fotos_by_muestra
        self._selected_fotos_by_muestra = {}
        for id_muestra, fotos in fotos_by_muestra.items():
            self._selected_fotos_by_muestra[id_muestra] = {str(foto.get("id_foto", "")) for foto in fotos if foto.get("id_foto")}

        for muestra in muestras:
            id_muestra = muestra["id_muestra"]
            fecha_hora = muestra.get("fecha_hora")
            fecha_texto = ""
            if hasattr(fecha_hora, "strftime"):
                fecha_texto = fecha_hora.strftime("%d/%m/%Y %H:%M")
            elif fecha_hora is not None:
                fecha_texto = str(fecha_hora)

            self.tree_muestras.insert(
                "",
                "end",
                iid=id_muestra,
                values=(
                    id_muestra,
                    muestra.get("nombre", ""),
                    muestra.get("cultivo", ""),
                    fecha_texto,
                    len(fotos_by_muestra.get(id_muestra, [])),
                ),
            )

        total_fotos = sum(len(v) for v in fotos_by_muestra.values())
        self.estado_var.set(
            f"Boleta {boleta}: {len(muestras)} muestra(s), {total_fotos} foto(s) en pantalla '{self.pantalla_var.get()}'."
        )

        if muestras:
            self.tree_muestras.selection_set(muestras[0]["id_muestra"])
            self.tree_muestras.focus(muestras[0]["id_muestra"])
            self._render_fotos_muestra(muestras[0]["id_muestra"])

    def _on_busqueda_error(self, exc: Exception) -> None:
        self.estado_var.set("Error al buscar boleta.")
        messagebox.showerror("Obtención calibres", f"No se pudo completar la búsqueda: {exc}", parent=self)

    def _on_select_muestra(self, _: tk.Event) -> None:
        selected = self.tree_muestras.selection()
        if not selected:
            return
        self._render_fotos_muestra(selected[0])

    def _clear_tree(self) -> None:
        for item in self.tree_muestras.get_children(""):
            self.tree_muestras.delete(item)

    def _limpiar_fotos(self) -> None:
        for child in self.frame_fotos_content.winfo_children():
            child.destroy()
        self._preview_refs = []
        self._fullsize_refs = []
        self._actualizar_resumen_fotos()

    def _render_fotos_muestra(self, id_muestra: str) -> None:
        self._limpiar_fotos()
        self._analysis_payload = {}
        self._deteccion_resultados = {}
        self._overlay_paths_by_foto = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._limpiar_resultados_ia()
        self._limpiar_resultados_estimacion_ia()
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._limpiar_resultados_comparacion_calibres()
        self._current_muestra_id = id_muestra
        self._current_cards = []
        fotos = self._fotos_by_muestra.get(id_muestra, [])

        if not fotos:
            ttk.Label(self.frame_fotos_content, text="No hay fotos para esta muestra.").grid(row=0, column=0, sticky="w", padx=6, pady=6)
            return

        url_base = self.data_service.get_url_base_servidor_fotos()
        if not url_base:
            ttk.Label(self.frame_fotos_content, text="No existe URL base de servidor configurada.").grid(row=0, column=0, sticky="w", padx=6, pady=6)
            return

        def worker() -> None:
            cards: list[dict[str, Any]] = []
            for foto in fotos:
                ruta_local = foto["ruta_local"].lstrip("/")
                url = f"{url_base}/fotos/{ruta_local}"
                error: str | None = None
                raw: bytes | None = None
                try:
                    raw = self.data_service.descargar_imagen(url)
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)
                cards.append({"foto": foto, "url": url, "raw": raw, "error": error})
            self.after(0, lambda: self._render_cards(cards))

        self.estado_var.set(f"Descargando {len(fotos)} foto(s) de la muestra {id_muestra}...")
        threading.Thread(target=worker, daemon=True).start()

    def _render_cards(self, cards: list[dict[str, Any]]) -> None:
        self._limpiar_fotos()
        self._current_cards = cards
        id_muestra = self._current_muestra_id
        if id_muestra is None:
            return
        seleccionadas = self._selected_fotos_by_muestra.setdefault(id_muestra, set())

        for idx, card in enumerate(cards):
            fila = idx // 3
            col = idx % 3
            box = ttk.Frame(self.frame_fotos_content, relief="ridge", padding=6)
            box.grid(row=fila, column=col, padx=6, pady=6, sticky="nsew")

            foto = card["foto"]
            id_foto = str(foto.get("id_foto", "")).strip()
            var_usar = tk.BooleanVar(value=id_foto in seleccionadas)
            check = ttk.Checkbutton(
                box,
                text="Usar en análisis",
                variable=var_usar,
                command=lambda v=var_usar, i=id_foto: self._on_toggle_foto(i, v.get()),
            )
            check.pack(anchor="w", pady=(0, 4))
            ttk.Label(box, text=f"Foto: {foto['id_foto']}", font=("Segoe UI", 9, "bold")).pack(anchor="w")
            ttk.Label(box, text=f"Ruta: {foto['ruta_local']}", wraplength=280).pack(anchor="w", pady=(2, 4))
            timestamp = foto.get("timestamp")
            ttk.Label(box, text=f"Timestamp: {timestamp if timestamp is not None else '-'}", wraplength=280, foreground="#4a4a4a").pack(anchor="w", pady=(0, 4))
            ia_resultado = self._get_ia_resultado_foto(id_foto)
            if ia_resultado:
                texto_ia = (
                    f"IA apta: {ia_resultado.get('apta', '-')}"
                    f" | Conf: {ia_resultado.get('confianza', '-')}"
                    f" | Oclusión: {ia_resultado.get('oclusion', '-')}"
                    f" | Patrón: {ia_resultado.get('patron_visible', '-')}"
                )
                ttk.Label(box, text=texto_ia, wraplength=280, foreground="#1f618d").pack(anchor="w", pady=(0, 4))
                estado_ia = str(ia_resultado.get("estado", "") or "").strip()
                if estado_ia:
                    color_estado = "#b00020" if ia_resultado.get("error") else "#1d8348"
                    ttk.Label(box, text=f"Estado IA: {estado_ia}", wraplength=280, foreground=color_estado).pack(anchor="w", pady=(0, 4))

            if card["error"]:
                ttk.Label(box, text=f"Error descarga: {card['error']}", foreground="#b00020", wraplength=280).pack(anchor="w")
                continue

            if Image is None or ImageTk is None:
                ttk.Label(box, text="PIL no disponible: no se pueden generar miniaturas.").pack(anchor="w")
                continue

            thumb = self._create_thumbnail(card["raw"])
            if thumb is None:
                ttk.Label(box, text="No fue posible renderizar miniatura.").pack(anchor="w")
                continue

            label_img = ttk.Label(box, image=thumb)
            label_img.image = thumb
            label_img.pack(anchor="w")
            label_img.bind("<Button-1>", lambda _event, c=card: self._abrir_vista_ampliada(c))
            self._preview_refs.append(thumb)
            ttk.Label(box, text=card["url"], wraplength=280, foreground="#1b4f72").pack(anchor="w", pady=(4, 0))

        self._actualizar_resumen_fotos()
        self._pintar_resultados_ia()
        self.estado_var.set(f"Fotos cargadas: {len(cards)}")

    def _create_thumbnail(self, raw: bytes | None) -> Any | None:
        if not raw or Image is None or ImageTk is None:
            return None
        try:
            with Image.open(io.BytesIO(raw)) as img:
                if ImageOps is not None:
                    img = ImageOps.exif_transpose(img)
                img.thumbnail((260, 260))
                return ImageTk.PhotoImage(img.copy())
        except Exception:
            return None

    def _limpiar_resultados_deteccion(self) -> None:
        for item in self.tree_resultados.get_children(""):
            self.tree_resultados.delete(item)

    def _limpiar_resultados_frutos(self) -> None:
        for item in self.tree_frutos_foto.get_children(""):
            self.tree_frutos_foto.delete(item)

    def _detectar_patron_y_escala(self) -> None:
        selected = self.tree_muestras.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para detectar patrón.", parent=self)
            return
        if self._detector is None:
            messagebox.showerror("Obtención calibres", "No hay configuración cargada para detección.", parent=self)
            return

        id_muestra = selected[0]
        ids_seleccionadas = self._selected_fotos_by_muestra.get(id_muestra, set())
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "No hay fotos seleccionadas para detección.", parent=self)
            return

        cards_by_id = {str(card.get("foto", {}).get("id_foto", "")): card for card in self._current_cards}
        self.estado_var.set(f"Ejecutando detección sobre {len(ids_seleccionadas)} imagen(es)...")

        def worker() -> None:
            resultados: dict[str, CircleDetectionResult] = {}
            overlays: dict[str, str] = {}
            for id_foto in sorted(ids_seleccionadas):
                card = cards_by_id.get(id_foto)
                if not card:
                    resultados[id_foto] = CircleDetectionResult(
                        image_id=id_foto,
                        detected=False,
                        diameter_px=None,
                        mm_per_pixel=None,
                        valid_for_next_step=False,
                        error="La foto no está cargada en memoria para procesar.",
                    )
                    continue

                result = self._detector.detect_from_bytes(id_foto, card.get("raw") or b"")
                resultados[id_foto] = result
                overlay_path = self._save_overlay_image(id_foto, card.get("raw") or b"", result)
                if overlay_path:
                    overlays[id_foto] = overlay_path

            self.after(0, lambda: self._on_detection_done(resultados, overlays))

        threading.Thread(target=worker, daemon=True).start()

    def _on_detection_done(self, resultados: dict[str, CircleDetectionResult], overlays: dict[str, str]) -> None:
        self._deteccion_resultados = resultados
        self._overlay_paths_by_foto = overlays
        self._pintar_resultados_deteccion(resultados)

    def _save_overlay_image(self, id_foto: str, raw_image: bytes, result: CircleDetectionResult) -> str | None:
        if self._detector is None:
            return None
        overlay_bytes = self._detector.build_overlay_bytes(raw_image, result)
        if not overlay_bytes:
            return None
        safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in id_foto) or "foto"
        path = self._overlay_dir / f"{safe_id}_overlay.png"
        try:
            path.write_bytes(overlay_bytes)
            return str(path)
        except Exception:
            return None

    def _pintar_resultados_deteccion(self, resultados: dict[str, CircleDetectionResult]) -> None:
        self._limpiar_resultados_deteccion()
        total = len(resultados)
        validas = 0
        for id_foto in sorted(resultados.keys()):
            res = resultados[id_foto]
            validas += 1 if res.valid_for_next_step else 0
            diametro = f"{res.diameter_px:.2f}" if res.diameter_px is not None else "-"
            mm_px = f"{res.mm_per_pixel:.5f}" if res.mm_per_pixel is not None else "-"
            detectado = "Sí" if res.detected else "No"
            valida = "Sí" if res.valid_for_next_step else "No"
            estado = "OK" if res.detected else (res.error or "Sin patrón")
            self.tree_resultados.insert("", "end", iid=id_foto, values=(id_foto, detectado, diametro, mm_px, valida, estado))
        invalidas = max(total - validas, 0)
        self.estado_var.set(f"Detección ejecutada: {total} imagen(es), válidas={validas}, inválidas={invalidas}.")
        self._actualizar_resumen_global()

    def _on_double_click_resultado(self, _: tk.Event) -> None:
        self._abrir_overlay_resultado_actual()

    def _abrir_overlay_resultado_actual(self) -> None:
        selected = self.tree_resultados.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione un resultado para abrir validación visual.", parent=self)
            return
        id_foto = selected[0]
        path = self._overlay_paths_by_foto.get(id_foto)
        if not path or not os.path.exists(path):
            messagebox.showwarning(
                "Obtención calibres",
                "No hay overlay disponible para esta foto. Vuelva a ejecutar detección.",
                parent=self,
            )
            return
        self._abrir_vista_ampliada_desde_archivo(path, id_foto)

    def _abrir_vista_ampliada_desde_archivo(self, image_path: str, id_foto: str) -> None:
        if Image is None or ImageTk is None:
            messagebox.showinfo("Obtención calibres", "PIL no disponible para abrir la validación visual.", parent=self)
            return

        try:
            with Image.open(image_path) as img:
                if ImageOps is not None:
                    img = ImageOps.exif_transpose(img)
                ancho, alto = img.size
                max_w, max_h = 1100, 800
                escala = min(max_w / max(ancho, 1), max_h / max(alto, 1), 1.0)
                nuevo_size = (max(int(ancho * escala), 1), max(int(alto * escala), 1))
                if nuevo_size != img.size:
                    img = img.resize(nuevo_size)
                photo = ImageTk.PhotoImage(img.copy())
        except Exception:
            messagebox.showerror("Obtención calibres", "No fue posible abrir la validación visual.", parent=self)
            return

        win = tk.Toplevel(self)
        win.title(f"Validación patrón - {id_foto}")
        cont = ttk.Frame(win, padding=8)
        cont.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        ttk.Label(cont, image=photo).grid(row=0, column=0, sticky="nsew")
        ttk.Label(cont, text=image_path, foreground="#1b4f72", wraplength=1000).grid(row=1, column=0, sticky="w", pady=(6, 0))
        self._fullsize_refs.append(photo)

    def _analizar_frutos(self) -> None:
        selected = self.tree_muestras.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para analizar frutos.", parent=self)
            return

        id_muestra = selected[0]
        ids_seleccionadas = self._selected_fotos_by_muestra.get(id_muestra, set())
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "No hay fotos seleccionadas para análisis de frutos.", parent=self)
            return
        if not self._deteccion_resultados:
            messagebox.showwarning("Obtención calibres", "Ejecute primero la detección de patrón para obtener mm/px.", parent=self)
            return

        muestra = next((item for item in self._muestras if item["id_muestra"] == id_muestra), None)
        cultivo = str(muestra.get("cultivo", "")).strip() if muestra else ""
        rangos = self._config.rangos_por_cultivo.get(cultivo, []) if self._config else []
        cards_by_id = {str(card.get("foto", {}).get("id_foto", "")): card for card in self._current_cards}

        self.estado_var.set("Analizando frutos y clasificando por calibre...")

        def worker() -> None:
            resultados: dict[str, PhotoFruitAnalysisResult] = {}
            overlays: dict[str, str] = {}
            for id_foto in sorted(ids_seleccionadas):
                escala = self._deteccion_resultados.get(id_foto)
                card = cards_by_id.get(id_foto)
                if escala is None or not escala.valid_for_next_step or escala.mm_per_pixel is None:
                    resultados[id_foto] = PhotoFruitAnalysisResult(
                        image_id=id_foto,
                        photo_valid_for_phase=False,
                        fruits=[],
                        caliber_count={},
                        caliber_percentage={},
                        discard_percentage=100.0,
                        error="Foto sin calibración válida (patrón/mm-px).",
                    )
                    continue
                if not card:
                    resultados[id_foto] = PhotoFruitAnalysisResult(
                        image_id=id_foto,
                        photo_valid_for_phase=False,
                        fruits=[],
                        caliber_count={},
                        caliber_percentage={},
                        discard_percentage=100.0,
                        error="Foto no disponible en memoria.",
                    )
                    continue
                result = self._fruit_analyzer.analyze_photo(
                    image_id=id_foto,
                    raw_image=card.get("raw") or b"",
                    mm_per_pixel=escala.mm_per_pixel,
                    caliber_ranges=rangos,
                )
                resultados[id_foto] = result
                overlay = self._save_fruit_overlay_image(id_foto, card.get("raw") or b"", result)
                if overlay:
                    overlays[id_foto] = overlay
            self.after(0, lambda: self._on_analisis_frutos_done(resultados, overlays))

        threading.Thread(target=worker, daemon=True).start()

    def _save_fruit_overlay_image(self, id_foto: str, raw_image: bytes, result: PhotoFruitAnalysisResult) -> str | None:
        overlay_bytes = self._fruit_analyzer.build_overlay_bytes(raw_image, result)
        if not overlay_bytes:
            return None
        safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in id_foto) or "foto"
        path = self._overlay_dir / f"{safe_id}_frutos_overlay.png"
        try:
            path.write_bytes(overlay_bytes)
            return str(path)
        except Exception:
            return None

    def _on_analisis_frutos_done(self, resultados: dict[str, PhotoFruitAnalysisResult], overlays: dict[str, str]) -> None:
        self._frutos_resultados = resultados
        self._frutos_overlay_paths_by_foto = overlays
        self._pintar_resultados_frutos()

    def _pintar_resultados_frutos(self) -> None:
        self._limpiar_resultados_frutos()
        total = len(self._frutos_resultados)
        validas_foto = 0
        total_validos = 0
        total_descartados = 0
        for id_foto in sorted(self._frutos_resultados.keys()):
            res = self._frutos_resultados[id_foto]
            detectados = len(res.fruits)
            validos = len([item for item in res.fruits if item.valid])
            descartados = max(detectados - validos, 0)
            total_validos += validos
            total_descartados += descartados
            validas_foto += 1 if res.photo_valid_for_phase else 0
            estado = "OK" if res.photo_valid_for_phase else (res.error or "Inválida")
            self.tree_frutos_foto.insert(
                "",
                "end",
                iid=id_foto,
                values=(id_foto, detectados, validos, descartados, f"{res.discard_percentage:.2f}", estado),
            )
        self.estado_var.set(
            f"Análisis frutos: fotos={total}, fotos válidas={validas_foto}, frutos válidos={total_validos}, descartados={total_descartados}."
        )
        self._actualizar_resumen_global()

    def _abrir_overlay_frutos_actual(self, _: tk.Event | None = None) -> None:
        selected = self.tree_frutos_foto.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione una fila de frutos para abrir el overlay.", parent=self)
            return
        id_foto = selected[0]
        path = self._frutos_overlay_paths_by_foto.get(id_foto)
        if not path or not os.path.exists(path):
            messagebox.showwarning("Obtención calibres", "No hay overlay de frutos para esta foto.", parent=self)
            return
        self._abrir_vista_ampliada_desde_archivo(path, id_foto)

    def _preparar_analisis_interno(self, show_message: bool) -> tuple[bool, str]:
        selected = self.tree_muestras.selection()
        if not selected:
            if show_message:
                messagebox.showinfo("Obtención calibres", "Seleccione una muestra para preparar análisis.", parent=self)
            return False, "Sin muestra seleccionada."

        id_muestra = selected[0]
        muestra = next((item for item in self._muestras if item["id_muestra"] == id_muestra), None)
        if not muestra:
            if show_message:
                messagebox.showerror("Obtención calibres", "No se encontró la muestra seleccionada.", parent=self)
            return False, "No se encontró la muestra seleccionada."

        cultivo = str(muestra.get("cultivo", "")).strip()
        rangos = []
        if self._config:
            rangos = self._config.rangos_por_cultivo.get(cultivo, [])
        fotos_muestra = self._fotos_by_muestra.get(id_muestra, [])
        ids_seleccionadas = self._selected_fotos_by_muestra.get(id_muestra, set())
        fotos_seleccionadas = [foto for foto in fotos_muestra if str(foto.get("id_foto", "")) in ids_seleccionadas]

        if not fotos_seleccionadas:
            if show_message:
                messagebox.showwarning(
                    "Obtención calibres",
                    "No hay fotos seleccionadas para el análisis. Seleccione al menos una foto.",
                    parent=self,
                )
            return False, "No hay fotos seleccionadas para preparar análisis."

        resultados = {k: v for k, v in self._deteccion_resultados.items() if k in ids_seleccionadas}
        fotos_validas = [
            foto
            for foto in fotos_seleccionadas
            if resultados.get(str(foto.get("id_foto", ""))) and resultados[str(foto.get("id_foto", ""))].valid_for_next_step
        ]

        if not fotos_validas:
            if show_message:
                messagebox.showwarning(
                    "Obtención calibres",
                    "Ninguna foto seleccionada pasó la detección del patrón. Ajuste selección o condiciones de captura.",
                    parent=self,
                )
            return False, "No hay fotos con patrón válido para preparar análisis."

        self._analysis_payload = {
            "id_muestra": id_muestra,
            "boleta": muestra.get("boleta", ""),
            "cultivo": cultivo,
            "diametro_patron_mm": self._config.diametro_patron_mm if self._config else 94.0,
            "rangos": rangos,
            "fotos": fotos_validas,
            "calibracion_imagenes": [
                resultados[id_foto].to_dict()
                for id_foto in sorted(resultados.keys())
            ],
            "analisis_frutos_por_foto": [
                self._frutos_resultados[id_foto].to_dict()
                for id_foto in sorted(self._frutos_resultados.keys())
                if id_foto in ids_seleccionadas
            ],
        }

        invalidas = len(resultados) - len(fotos_validas)
        if show_message:
            messagebox.showinfo(
                "Obtención calibres",
                (
                    "Preparación lista para análisis de calibres.\n\n"
                    f"Muestra: {id_muestra}\n"
                    f"Cultivo: {cultivo or '-'}\n"
                    f"Fotos válidas: {len(self._analysis_payload['fotos'])}\n"
                    f"Fotos inválidas: {invalidas}\n"
                    f"Diámetro patrón: {self._analysis_payload['diametro_patron_mm']:.2f} mm\n"
                    f"Rangos configurados: {len(rangos)}"
                ),
                parent=self,
            )
        return True, "Preparación completada."

    def _preparar_analisis(self) -> None:
        self._preparar_analisis_interno(show_message=True)

    def get_analysis_payload(self) -> dict[str, Any]:
        """Expone el payload armado para la siguiente etapa (cálculo de calibres)."""
        return dict(self._analysis_payload)

    def _ejecutar_validacion_ia(self) -> None:
        if self._ai_validacion_en_curso:
            return
        if self._ai_lote_en_curso:
            return
        if not self._current_muestra_id:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para ejecutar Validación IA.", parent=self)
            return

        ids_seleccionadas = sorted(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "Marque una foto para ejecutar Validación IA.", parent=self)
            return
        if len(ids_seleccionadas) != 1:
            messagebox.showinfo(
                "Obtención calibres",
                "La validación experimental IA se ejecuta sobre una sola foto. Deje marcada únicamente una.",
                parent=self,
            )
            return

        id_foto = ids_seleccionadas[0]
        card = next((c for c in self._current_cards if str(c.get("foto", {}).get("id_foto", "")) == id_foto), None)
        if not card:
            messagebox.showerror("Obtención calibres", "La foto seleccionada no está disponible en memoria.", parent=self)
            return

        ruta_local = str(card.get("foto", {}).get("ruta_local", "")).strip()
        if not ruta_local:
            messagebox.showwarning(
                "Obtención calibres",
                (
                    "La foto seleccionada no tiene 'ruta_local'.\n"
                    "Si la imagen solo existe por URL, hay que mapear/descargar primero a una ruta accesible por el servidor interno."
                ),
                parent=self,
            )
            return

        image_url_for_ai = self._build_image_url_for_ai(ruta_local)
        if not image_url_for_ai:
            messagebox.showerror(
                "Obtención calibres - Validación IA",
                (
                    "No se pudo construir la URL HTTP de la imagen.\n"
                    f"id_foto: {id_foto}\n"
                    f"ruta_local: {ruta_local}\n"
                    "Revise ServidorFotos/url_actual/url en Firestore."
                ),
                parent=self,
            )
            LOGGER.error("Validación IA: no fue posible construir image_url. id_foto=%s ruta_local=%s", id_foto, ruta_local)
            return

        service_url, source, resolve_error = self.data_service.resolve_url_servicio_ia()
        if not service_url:
            LOGGER.error("Validación IA: URL de servicio no resuelta. source=%s error=%s", source, resolve_error)
            messagebox.showerror(
                "Obtención calibres",
                (
                    "No hay URL de servicio IA.\n"
                    "Orden de resolución: HARVESTSYNC_INTERNAL_AI_URL -> ServidorIA/url_actual/url.\n"
                    f"Detalle: {resolve_error or 'No hay configuración disponible.'}"
                ),
                parent=self,
            )
            return
        LOGGER.info(
            "Validación IA: preparado request. service_url=%s source=%s id_foto=%s ruta_local=%s image_url_enviada=%s",
            service_url,
            source,
            id_foto,
            ruta_local,
            image_url_for_ai,
        )

        self._ai_validacion_en_curso = True
        self.btn_validacion_ia.config(state="disabled")
        self.estado_var.set(f"Validación IA en curso para foto {id_foto}...")
        timeout_seconds = 25
        cultivo = self._resolver_cultivo_ia()
        variedad = self._resolver_variedad_ia()

        def worker() -> None:
            t0 = time.perf_counter()
            LOGGER.info(
                "Validación IA: inicio llamada. base_url=%s endpoint=%s/analyze-image id_foto=%s timeout=%ss image_url=%s",
                service_url.rstrip("/"),
                service_url.rstrip("/"),
                id_foto,
                timeout_seconds,
                image_url_for_ai,
            )
            try:
                result = call_analyze_image(
                    server_url=service_url,
                    image_url=image_url_for_ai,
                    task="validacion_foto",
                    context=(
                        "Evaluar utilidad de imagen para calibres: "
                        "visibilidad general, oclusión, nitidez y presencia/claridad del patrón."
                    ),
                    cultivo=cultivo,
                    variedad=variedad,
                    timeout_seconds=timeout_seconds,
                )
                elapsed = time.perf_counter() - t0
                LOGGER.info("Validación IA: fin correcto. id_foto=%s duracion=%.2fs", id_foto, elapsed)
                self.after(0, lambda: self._on_validacion_ia_ok(id_foto=id_foto, image_ref=image_url_for_ai, result=result))
            except InternalAIClientError as exc:
                elapsed = time.perf_counter() - t0
                LOGGER.error("Validación IA: error del servicio interno. id_foto=%s duracion=%.2fs error=%s", id_foto, elapsed, exc)
                error_message = str(exc)
                if "HTTP 404" in error_message:
                    error_message = (
                        "HTTP 404 en servicio interno.\n"
                        f"Base URL: {service_url.rstrip('/')}\n"
                        "Endpoint esperado: /analyze-image\n"
                        f"id_foto: {id_foto}\n"
                        f"image_url: {image_url_for_ai}\n\n"
                        "Revise que el servicio levantado corresponda al servicio IA interno de HarvestSync."
                    )
                self.after(0, lambda error=error_message: self._on_validacion_ia_error(error))
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Validación IA: error inesperado en llamada al servicio.")
                self.after(0, lambda error=exc: self._on_validacion_ia_error(f"Error inesperado: {error}"))

        threading.Thread(target=worker, daemon=True).start()

    def _on_validacion_ia_ok(self, id_foto: str, image_ref: str, result: dict[str, Any]) -> None:
        self._ai_validacion_en_curso = False
        self.btn_validacion_ia.config(state="normal")
        self.estado_var.set(f"Validación IA completada para {id_foto}.")
        parsed = self._parse_validacion_ia_result(result)
        resultados = self._get_ia_resultados_muestra_actual()
        resultados[id_foto] = {
            "apta": parsed.get("apta", "-"),
            "confianza": parsed.get("confianza", "-"),
            "oclusion": parsed.get("oclusion", "-"),
            "patron_visible": parsed.get("patron_visible", "-"),
            "estado": "OK",
            "error": False,
            "image_url": image_ref,
            "raw_result": result,
            "parsed": parsed,
        }
        self._pintar_resultados_ia()
        self._mostrar_resultado_ia(id_foto=id_foto, image_ref=image_ref, result=result)

    def _on_validacion_ia_error(self, error_message: str) -> None:
        self._ai_validacion_en_curso = False
        self.btn_validacion_ia.config(state="normal")
        self.estado_var.set("Validación IA con error.")
        messagebox.showerror("Obtención calibres - Validación IA", error_message, parent=self)

    def _build_image_url_for_ai(self, ruta_local: str) -> str:
        """Construye URL HTTP de imagen reutilizando el patrón actual de la herramienta."""
        url_base = self.data_service.get_url_base_servidor_fotos().strip().rstrip("/")
        ruta_limpia = ruta_local.lstrip("/\\")
        if not url_base or not ruta_limpia:
            return ""
        return f"{url_base}/fotos/{ruta_limpia}"

    @staticmethod
    def _parse_validacion_ia_result(result: dict[str, Any]) -> dict[str, Any]:
        """Parsea result['output_text'] como JSON y devuelve datos listos para UI."""
        output_text = result.get("output_text", "")
        parsed_output = ObtencionCalibresWindow._parse_output_json(output_text)
        alertas = parsed_output.get("alertas", [])
        if isinstance(alertas, str):
            alertas = [alertas]
        if not isinstance(alertas, list):
            alertas = [str(alertas)]

        apta_val = parsed_output.get("apta")
        if isinstance(apta_val, bool):
            apta_texto = "Sí" if apta_val else "No"
        else:
            apta_texto = "-"

        return {
            "apta": apta_texto,
            "confianza": parsed_output.get("confianza", "-"),
            "oclusion": parsed_output.get("oclusion", "-"),
            "patron_visible": parsed_output.get("patron_visible", "-"),
            "box_centrado": parsed_output.get("box_centrado", "-"),
            "resumen": parsed_output.get("resumen", "-"),
            "alertas": alertas,
            "recomendacion": parsed_output.get("recomendacion", "-"),
            "modelo": result.get("model", "-"),
            "raw_id": result.get("raw_id", "-"),
            "json_parse_ok": bool(parsed_output),
            "output_text_original": output_text,
            "json_parseado": parsed_output,
        }

    @staticmethod
    def _parse_output_json(output_text: Any) -> dict[str, Any]:
        if isinstance(output_text, dict):
            return output_text
        if not isinstance(output_text, str):
            return {}
        output_text_clean = output_text.strip()
        if not output_text_clean:
            return {}

        candidate_texts = [output_text_clean]
        if "```" in output_text_clean:
            parts = output_text_clean.split("```")
            for part in parts:
                part_clean = part.strip()
                if not part_clean:
                    continue
                if part_clean.lower().startswith("json"):
                    part_clean = part_clean[4:].strip()
                candidate_texts.append(part_clean)

        for candidate in candidate_texts:
            try:
                first_pass = json.loads(candidate)
                if isinstance(first_pass, dict):
                    return first_pass
                if isinstance(first_pass, str):
                    second_pass = json.loads(first_pass)
                    if isinstance(second_pass, dict):
                        return second_pass
            except json.JSONDecodeError:
                continue
        return {}

    @staticmethod
    def _parse_json_ia_output(output_text: Any) -> tuple[dict[str, Any] | None, str | None]:
        if isinstance(output_text, dict):
            return output_text, None
        if not isinstance(output_text, str):
            return None, "output_text no es string ni objeto JSON."

        output_text_clean = output_text.strip()
        if not output_text_clean:
            return None, "output_text vacío."

        candidates = [output_text_clean]
        if "```" in output_text_clean:
            for part in output_text_clean.split("```"):
                part_clean = part.strip()
                if not part_clean:
                    continue
                if part_clean.lower().startswith("json"):
                    part_clean = part_clean[4:].strip()
                candidates.append(part_clean)

        last_error = "JSON IA no parseable."
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed, None
                if isinstance(parsed, str):
                    parsed_nested = json.loads(parsed)
                    if isinstance(parsed_nested, dict):
                        return parsed_nested, None
            except json.JSONDecodeError as exc:
                last_error = f"JSON IA no parseable: {exc.msg} (pos {exc.pos})."
        return None, last_error

    @staticmethod
    def _normalizar_estimacion_calibres_ia(parsed: dict[str, Any]) -> dict[str, Any]:
        advertencias_raw = parsed.get("advertencias", [])
        if isinstance(advertencias_raw, str):
            advertencias = [advertencias_raw]
        elif isinstance(advertencias_raw, list):
            advertencias = [str(item).strip() for item in advertencias_raw if str(item).strip()]
        else:
            advertencias = [str(advertencias_raw).strip()] if str(advertencias_raw).strip() else []

        distribucion: list[dict[str, Any]] = []
        distribucion_raw = parsed.get("distribucion", [])
        if isinstance(distribucion_raw, list):
            for item in distribucion_raw:
                if not isinstance(item, dict):
                    continue
                calibre = str(item.get("calibre", "")).strip()
                porcentaje = ObtencionCalibresWindow._confianza_a_float(item.get("porcentaje"))
                if not calibre:
                    continue
                distribucion.append(
                    {
                        "calibre": calibre,
                        "porcentaje": 0.0 if porcentaje is None else max(0.0, min(100.0, porcentaje)),
                    }
                )

        apta_raw = parsed.get("apta_para_estimacion")
        if isinstance(apta_raw, bool):
            apta_texto = "Sí" if apta_raw else "No"
        else:
            apta_texto = "-"

        campos_clave = (
            "apta_para_estimacion",
            "confianza",
            "frutos_visibles_estimados",
            "calibre_dominante",
            "distribucion",
        )
        faltantes = [campo for campo in campos_clave if campo not in parsed]
        es_valida = not faltantes and apta_texto in ("Sí", "No")

        return {
            "apta_para_estimacion": apta_texto,
            "confianza": parsed.get("confianza", "-"),
            "frutos_visibles_estimados": parsed.get("frutos_visibles_estimados", "-"),
            "calibre_dominante": parsed.get("calibre_dominante", "-"),
            "distribucion": distribucion,
            "advertencias": advertencias,
            "resumen": parsed.get("resumen", "-"),
            "campos_faltantes": faltantes,
            "es_valida": es_valida,
        }

    @staticmethod
    def _parse_estimacion_ia_result(result: dict[str, Any]) -> dict[str, Any]:
        output_text = result.get("output_text", "")
        parsed_output, parse_error = ObtencionCalibresWindow._parse_json_ia_output(output_text)
        if parsed_output is None and isinstance(result.get("json_parseado"), dict):
            parsed_output = result.get("json_parseado")
            parse_error = None
        if parsed_output is None and isinstance(result.get("output"), dict):
            parsed_output = result.get("output")
            parse_error = None

        if not isinstance(parsed_output, dict):
            return {
                "apta_para_estimacion": "-",
                "confianza": "-",
                "frutos_visibles_estimados": "-",
                "calibre_dominante": "-",
                "distribucion": [],
                "advertencias": [],
                "resumen": "-",
                "json_parse_ok": False,
                "json_parseado": None,
                "output_text_original": output_text,
                "campos_faltantes": [],
                "diagnostico": parse_error or "JSON IA no parseable",
                "error_tipo": "parse",
                "es_valida": False,
            }

        normalized = ObtencionCalibresWindow._normalizar_estimacion_calibres_ia(parsed_output)
        faltantes = normalized.get("campos_faltantes", [])
        es_valida = bool(normalized.get("es_valida"))
        diagnostico = ""
        error_tipo = ""
        if not es_valida:
            diagnostico = "Respuesta IA sin campos esperados"
            error_tipo = "campos"

        return {
            **normalized,
            "json_parse_ok": True,
            "json_parseado": parsed_output,
            "output_text_original": output_text,
            "diagnostico": diagnostico,
            "error_tipo": error_tipo,
        }

    def _get_ia_resultados_muestra_actual(self) -> dict[str, dict[str, Any]]:
        if not self._current_muestra_id:
            return {}
        return self._ia_validacion_resultados_by_muestra.setdefault(self._current_muestra_id, {})

    def _get_ia_resultado_foto(self, id_foto: str) -> dict[str, Any] | None:
        if not self._current_muestra_id:
            return None
        return self._ia_validacion_resultados_by_muestra.get(self._current_muestra_id, {}).get(id_foto)

    def _limpiar_resultados_ia(self) -> None:
        if hasattr(self, "tree_validacion_ia"):
            for item in self.tree_validacion_ia.get_children(""):
                self.tree_validacion_ia.delete(item)
        if hasattr(self, "resumen_ia_lote_var"):
            self.resumen_ia_lote_var.set("IA lote: evaluadas=0 | aptas=0 | no aptas=0 | errores=0 | confianza media=-")
        self._actualizar_resumen_global()

    def _get_estimacion_resultados_muestra_actual(self) -> dict[str, dict[str, Any]]:
        if not self._current_muestra_id:
            return {}
        return self._ia_estimacion_resultados_by_muestra.setdefault(self._current_muestra_id, {})

    def _limpiar_resultados_estimacion_ia(self) -> None:
        if hasattr(self, "tree_estimacion_ia"):
            for item in self.tree_estimacion_ia.get_children(""):
                self.tree_estimacion_ia.delete(item)
        if hasattr(self, "resumen_estimacion_ia_var"):
            self.resumen_estimacion_ia_var.set(
                "Estimación IA experimental: evaluadas=0 | aptas=0 | confianza media=- | distribución consolidada=-"
            )
        if hasattr(self, "advertencias_estimacion_ia_var"):
            self.advertencias_estimacion_ia_var.set("Advertencias estimación IA experimental: -")
        self._actualizar_resumen_global()

    def _limpiar_resultados_comparacion_calibres(self) -> None:
        if hasattr(self, "tree_comparacion_calibres"):
            for item in self.tree_comparacion_calibres.get_children(""):
                self.tree_comparacion_calibres.delete(item)
        if hasattr(self, "resumen_calibrador_var"):
            self.resumen_calibrador_var.set(
                "Calibrador total partida: Podrido=- | DesLínea=- | DesMesa=- | Destrío total=- | ΣCAL0..CAL9=-"
            )
        if hasattr(self, "resumen_metricas_comparacion_var"):
            self.resumen_metricas_comparacion_var.set(
                "Métricas: error absoluto medio=- | error total absoluto=- | dominante IA=- | dominante real normalizado=-"
            )
        if hasattr(self, "nota_comparacion_var"):
            self.nota_comparacion_var.set(
                "Comparación realizada sobre distribución de calibres normalizada, excluyendo podrido y destrío."
            )
        self._comparacion_ia_vs_calibrador = {}
        self._contexto_comparacion_actual = {}

    def _limpiar_selector_entregas(self) -> None:
        self._entregas_calibrador = []
        self._boleta_entregas_calibrador = ""
        self._selector_entrega_map = {OPCION_BOLETA_COMPLETA: None}
        if hasattr(self, "combo_entrega_calibrador"):
            self.combo_entrega_calibrador.configure(values=[OPCION_BOLETA_COMPLETA])
        self.selector_entrega_var.set(OPCION_BOLETA_COMPLETA)
        self._actualizar_texto_contexto_comparacion()

    @staticmethod
    def _parse_fecha_entrega(entrega: dict[str, Any]) -> str:
        for campo in ("Fcarga", "FRecoleccion"):
            value = entrega.get(campo)
            if value in (None, ""):
                continue
            if hasattr(value, "strftime"):
                return value.strftime("%d/%m/%Y")
            texto = str(value).strip()
            if not texto:
                continue
            if len(texto) >= 10 and texto[4] == "-" and texto[7] == "-":
                return f"{texto[8:10]}/{texto[5:7]}/{texto[0:4]}"
            return texto
        return "-"

    def _formatear_opcion_entrega(self, entrega: dict[str, Any]) -> str:
        albaran = str(entrega.get("AlbaranDef") or entrega.get("Albaran") or entrega.get("Albaran2") or "-").strip() or "-"
        fecha = self._parse_fecha_entrega(entrega)
        neto = _valor_a_float(entrega.get("neto"), 0.0)
        variedad = str(entrega.get("Variedad", "") or "-").strip() or "-"
        return f"{albaran} | Fecha {fecha} | Neto {neto:.3f} | Variedad {variedad}"

    def _actualizar_texto_contexto_comparacion(self) -> None:
        clave = self.selector_entrega_var.get().strip() or OPCION_BOLETA_COMPLETA
        if clave == OPCION_BOLETA_COMPLETA:
            self.contexto_comparacion_var.set("Contexto: Boleta completa ponderada por Neto.")
            return
        entrega = self._selector_entrega_map.get(clave)
        if not entrega:
            self.contexto_comparacion_var.set("Contexto: entrega no encontrada.")
            return
        self.contexto_comparacion_var.set(f"Contexto: Entrega concreta -> {self._formatear_opcion_entrega(entrega)}")

    def _resolver_variedad_ia(self) -> str:
        clave = self.selector_entrega_var.get().strip() or OPCION_BOLETA_COMPLETA
        if clave != OPCION_BOLETA_COMPLETA:
            entrega = self._selector_entrega_map.get(clave) or {}
            variedad = str(entrega.get("Variedad", "") or "").strip().upper()
            return variedad or "*"
        variedades = {
            str(entrega.get("Variedad", "") or "").strip().upper()
            for entrega in self._entregas_calibrador
            if isinstance(entrega, dict)
        }
        variedades = {item for item in variedades if item}
        if len(variedades) == 1:
            return next(iter(variedades))
        return "*"

    def _resolver_cultivo_ia(self) -> str:
        muestra = next((item for item in self._muestras if item["id_muestra"] == self._current_muestra_id), None)
        cultivo = str(muestra.get("cultivo", "")).strip().upper() if muestra else ""
        return cultivo or "*"

    def _cargar_entregas_calibrador(self) -> None:
        boleta = self._get_boleta_actual()
        if not boleta:
            messagebox.showinfo("Obtención calibres", "Ingrese o cargue una boleta antes de consultar entregas.", parent=self)
            return
        self.estado_var.set(f"Cargando entregas de calibrador para boleta {boleta}...")

        def worker() -> None:
            try:
                entregas = listar_entregas_por_boleta(DB_FRUTA_PATH, boleta)
                self.after(0, lambda: self._on_cargar_entregas_calibrador_ok(boleta, entregas))
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: self._on_cargar_entregas_calibrador_error(error))

        threading.Thread(target=worker, daemon=True).start()

    def _on_cargar_entregas_calibrador_ok(self, boleta: str, entregas: list[dict[str, Any]]) -> None:
        self._entregas_calibrador = entregas
        self._boleta_entregas_calibrador = boleta
        self._aplicar_entregas_selector(entregas)

        if not entregas:
            self.estado_var.set(f"Boleta {boleta}: sin entregas en calibrador con filtros CAMPAÑA/EMPRESA/CULTIVO.")
            messagebox.showwarning(
                "Obtención calibres",
                f"No hay entregas para boleta {boleta} con CAMPAÑA={FILTRO_CALIBRADOR_CAMPANA}, "
                f"EMPRESA={FILTRO_CALIBRADOR_EMPRESA}, CULTIVO={FILTRO_CALIBRADOR_CULTIVO}.",
                parent=self,
            )
            return

        self.estado_var.set(f"Entregas calibrador cargadas: {len(entregas)} para boleta {boleta}.")

    def _aplicar_entregas_selector(self, entregas: list[dict[str, Any]]) -> None:
        seleccion_actual = self.selector_entrega_var.get().strip()
        self._selector_entrega_map = {OPCION_BOLETA_COMPLETA: None}
        values = [OPCION_BOLETA_COMPLETA]
        for entrega in entregas:
            key = f"Entrega {int(entrega.get('idx', 0)) + 1}: {self._formatear_opcion_entrega(entrega)}"
            self._selector_entrega_map[key] = entrega
            values.append(key)
        self.combo_entrega_calibrador.configure(values=values)

        if not entregas:
            self.selector_entrega_var.set(OPCION_BOLETA_COMPLETA)
            self._actualizar_texto_contexto_comparacion()
            return
        if seleccion_actual and seleccion_actual in self._selector_entrega_map:
            self.selector_entrega_var.set(seleccion_actual)
            self._actualizar_texto_contexto_comparacion()
            return

        if len(entregas) == 1:
            self.selector_entrega_var.set(values[1])
        else:
            self.selector_entrega_var.set(OPCION_BOLETA_COMPLETA)
        self._actualizar_texto_contexto_comparacion()

    def _on_cargar_entregas_calibrador_error(self, exc: Exception) -> None:
        self.estado_var.set("Error al cargar entregas de calibrador.")
        messagebox.showerror("Obtención calibres", f"No se pudieron cargar entregas: {exc}", parent=self)

    @staticmethod
    def _confianza_a_float(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            limpio = value.strip().replace("%", "").replace(",", ".")
            try:
                return float(limpio)
            except ValueError:
                return None
        return None

    def _get_boleta_actual(self) -> str:
        if self._current_muestra_id:
            for muestra in self._muestras:
                if muestra.get("id_muestra") == self._current_muestra_id:
                    return str(muestra.get("boleta", "")).strip()
        return self.boleta_var.get().strip()

    def _get_distribucion_ia_consolidada(self) -> dict[str, float]:
        resultados = self._get_estimacion_resultados_muestra_actual()
        acumulado: dict[str, float] = {}
        count = 0
        for row in resultados.values():
            if row.get("error") or row.get("apta_para_estimacion") != "Sí":
                continue
            distribucion = row.get("distribucion", [])
            if not isinstance(distribucion, list):
                continue
            count += 1
            for item in distribucion:
                calibre = str(item.get("calibre", "")).strip().upper()
                pct = self._confianza_a_float(item.get("porcentaje"))
                if not calibre or pct is None:
                    continue
                acumulado[calibre] = acumulado.get(calibre, 0.0) + max(0.0, pct)
        if count <= 0 or not acumulado:
            return {}
        promedio = {calibre: valor / count for calibre, valor in acumulado.items()}
        total = sum(promedio.values())
        if total <= 0:
            return {}
        return {calibre: (valor * 100.0 / total) for calibre, valor in promedio.items()}

    def _comparar_ia_vs_calibrador(self) -> None:
        boleta = self._get_boleta_actual()
        if not boleta:
            messagebox.showinfo("Obtención calibres", "No hay boleta seleccionada para comparar.", parent=self)
            return
        distribucion_ia = self._get_distribucion_ia_consolidada()
        if not distribucion_ia:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay distribución IA apta para comparar. Ejecute 'Estimación calibres IA'.",
                parent=self,
            )
            return
        try:
            if self._boleta_entregas_calibrador != boleta:
                self._entregas_calibrador = listar_entregas_por_boleta(DB_FRUTA_PATH, boleta)
                self._boleta_entregas_calibrador = boleta
            if not self._entregas_calibrador:
                raise ValueError(
                    f"No hay entregas para boleta {boleta} con filtros CAMPAÑA={FILTRO_CALIBRADOR_CAMPANA}, "
                    f"EMPRESA={FILTRO_CALIBRADOR_EMPRESA}, CULTIVO={FILTRO_CALIBRADOR_CULTIVO}."
                )
            self._aplicar_entregas_selector(self._entregas_calibrador)
            clave_selector = self.selector_entrega_var.get().strip() or OPCION_BOLETA_COMPLETA
            if clave_selector == OPCION_BOLETA_COMPLETA:
                calibrador = cargar_calibrador_boleta_ponderado(self._entregas_calibrador)
                contexto = "Boleta completa ponderada por Neto"
            else:
                entrega = self._selector_entrega_map.get(clave_selector)
                if not entrega:
                    raise ValueError("La entrega seleccionada no está disponible. Recargue entregas.")
                calibrador = cargar_calibrador_por_entrega(entrega)
                contexto = f"Entrega concreta ({self._formatear_opcion_entrega(entrega)})"
            real_normalizado = normalizar_distribucion_calibres(calibrador["calibres_brutos"])
        except Exception as exc:  # noqa: BLE001
            self._limpiar_resultados_comparacion_calibres()
            self.nota_comparacion_var.set(str(exc))
            messagebox.showerror("Obtención calibres", f"No se pudo comparar IA vs calibrador: {exc}", parent=self)
            return

        self._limpiar_resultados_comparacion_calibres()
        comp = comparar_distribuciones(distribucion_ia, real_normalizado)
        self._comparacion_ia_vs_calibrador = {"boleta": boleta, **comp}
        entrega_ctx = None if clave_selector == OPCION_BOLETA_COMPLETA else self._selector_entrega_map.get(clave_selector)
        self._contexto_comparacion_actual = {
            "contexto": contexto,
            "entrega": entrega_ctx,
            "tipo_comparacion": "boleta_completa" if entrega_ctx is None else "entrega",
            "calibrador": calibrador,
            "real_normalizado": real_normalizado,
        }
        self.resumen_calibrador_var.set(
            "Calibrador total partida: "
            f"Podrido={calibrador['podrido']:.2f}% | DesLínea={calibrador['des_linea']:.2f}% | "
            f"DesMesa={calibrador['des_mesa']:.2f}% | Destrío total={calibrador['destrio_total']:.2f}% | "
            f"ΣCAL0..CAL9={calibrador['suma_calibres']:.2f}%"
        )
        for calibre in CALIBRES_CALIBRADOR:
            self.tree_comparacion_calibres.insert(
                "",
                "end",
                values=(
                    calibre,
                    f"{distribucion_ia.get(calibre, 0.0):.2f}",
                    f"{real_normalizado.get(calibre, 0.0):.2f}",
                    f"{calibrador['calibres_brutos'].get(calibre, 0.0):.2f}",
                    f"{abs(distribucion_ia.get(calibre, 0.0) - real_normalizado.get(calibre, 0.0)):.2f}",
                ),
            )
        self.resumen_metricas_comparacion_var.set(
            "Métricas: "
            f"error absoluto medio={comp['error_abs_medio']:.2f} | "
            f"error total absoluto={comp['error_total_abs']:.2f} | "
            f"dominante IA={comp['calibre_dominante_ia']} | "
            f"dominante real normalizado={comp['calibre_dominante_real']}"
        )
        self.nota_comparacion_var.set(
            f"Comparación sobre {contexto}. Distribución real normalizada en CAL0..CAL9; "
            "podrido/destrío no forman parte de dicha normalización."
        )

    @staticmethod
    def _to_int_or_none(value: Any) -> int | None:
        if value is None:
            return None
        texto = str(value).strip()
        if not texto:
            return None
        try:
            return int(float(texto.replace(",", ".")))
        except ValueError:
            return None

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        texto = str(value).strip().replace("%", "").replace(",", ".")
        if not texto:
            return None
        try:
            return float(texto)
        except ValueError:
            return None

    @staticmethod
    def _normalizar_distribucion_por_foto(distribucion: list[dict[str, Any]]) -> dict[str, float]:
        acumulado = {f"CAL {idx}": 0.0 for idx in range(10)}
        for item in distribucion or []:
            calibre = str(item.get("calibre", "")).strip().upper()
            if calibre not in acumulado:
                continue
            pct = ObtencionCalibresWindow._to_float_or_none(item.get("porcentaje"))
            if pct is None:
                continue
            acumulado[calibre] += max(0.0, pct)
        total = sum(acumulado.values())
        if total <= 0:
            return acumulado
        return {calibre: (valor * 100.0 / total) for calibre, valor in acumulado.items()}

    def _build_historial_row(self, id_foto: str, estimacion: dict[str, Any]) -> dict[str, Any]:
        comparacion = self._comparacion_ia_vs_calibrador
        contexto = self._contexto_comparacion_actual
        if not comparacion or not contexto:
            raise ValueError("Primero compare IA vs calibrador para generar métricas históricas.")

        boleta = self._get_boleta_actual()
        entrega = contexto.get("entrega")
        if entrega is None:
            entrega = self._entregas_calibrador[0] if self._entregas_calibrador else {}

        calibrador = contexto.get("calibrador", {})
        real_norm = contexto.get("real_normalizado", {})
        distribucion_foto = self._normalizar_distribucion_por_foto(estimacion.get("distribucion", []))
        comp_foto = comparar_distribuciones(distribucion_foto, real_norm)

        card = next((item for item in self._current_cards if str(item.get("foto", {}).get("id_foto", "")) == id_foto), {})
        ruta_local = str(card.get("foto", {}).get("ruta_local", "")).strip()
        output_json = estimacion.get("json_parseado")
        output_json_text = json.dumps(output_json, ensure_ascii=False) if isinstance(output_json, dict) else str(
            estimacion.get("output_text_original", "") or ""
        )
        advertencias = estimacion.get("advertencias", [])
        advertencias_texto = " | ".join(str(item).strip() for item in advertencias if str(item).strip())
        raw_result = estimacion.get("raw_result", {})
        prompt_version = str(raw_result.get("prompt_version", "") or "").strip() or PROMPT_VERSION
        prompt_source = str(raw_result.get("prompt_source", "") or "").strip() or "no_informado"
        cultivo_ia = str(raw_result.get("cultivo", "") or "").strip()
        variedad_ia = str(raw_result.get("variedad", "") or "").strip()

        row = {
            "fecha_registro": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "boleta": self._to_int_or_none(boleta),
            "albaran": str(
                entrega.get("AlbaranDef") or entrega.get("Albaran") or entrega.get("Albaran2") or OPCION_BOLETA_COMPLETA
            ).strip(),
            "tipo_comparacion": str(contexto.get("tipo_comparacion", "") or "").strip(),
            "campana": self._to_int_or_none(entrega.get("Campana")),
            "empresa": self._to_int_or_none(entrega.get("Empresa")),
            "cultivo": cultivo_ia or str(entrega.get("Cultivo", "") or "").strip(),
            "variedad": variedad_ia or str(entrega.get("Variedad", "") or "").strip(),
            "socio": str(entrega.get("Socio", "") or "").strip(),
            "id_socio": self._to_int_or_none(entrega.get("IdSocio")),
            "neto": self._to_float_or_none(entrega.get("Neto")),
            "id_foto": id_foto,
            "image_url": str(estimacion.get("image_url", "") or "").strip(),
            "ruta_local": ruta_local,
            "modelo_ia": str(raw_result.get("model", "") or "").strip() or "desconocido",
            "prompt_version": prompt_version,
            "prompt_source": prompt_source,
            "confianza_ia": self._to_float_or_none(estimacion.get("confianza")),
            "calibre_dominante_ia": str(comp_foto.get("calibre_dominante_ia", "") or "").strip(),
            "calibre_dominante_real": str(comp_foto.get("calibre_dominante_real", "") or "").strip(),
            "error_absoluto_medio": self._to_float_or_none(comp_foto.get("error_abs_medio")),
            "error_total_absoluto": self._to_float_or_none(comp_foto.get("error_total_abs")),
            "podrido": self._to_float_or_none(calibrador.get("podrido")),
            "deslinea": self._to_float_or_none(calibrador.get("des_linea")),
            "desmesa": self._to_float_or_none(calibrador.get("des_mesa")),
            "destrio_total": self._to_float_or_none(calibrador.get("destrio_total")),
            "suma_calibres_real": self._to_float_or_none(calibrador.get("suma_calibres")),
            "advertencias_ia": advertencias_texto,
            "resumen_ia": str(estimacion.get("resumen", "") or "").strip(),
            "output_ia_json": output_json_text,
            "observaciones": str(estimacion.get("diagnostico", "") or "").strip(),
        }
        for idx in range(10):
            calibre = f"CAL {idx}"
            row[f"ia_cal{idx}"] = distribucion_foto.get(calibre, 0.0)
            row[f"real_norm_cal{idx}"] = real_norm.get(calibre, 0.0)
            row[f"real_bruto_cal{idx}"] = calibrador.get("calibres_brutos", {}).get(calibre, 0.0)
        return row

    def _guardar_comparacion_historico(self) -> None:
        if self._ai_estimacion_en_curso or self._ai_lote_en_curso or self._ai_validacion_en_curso:
            messagebox.showinfo("Obtención calibres", "Espere a que finalicen los procesos IA antes de guardar.", parent=self)
            return
        if not self._comparacion_ia_vs_calibrador or not self._contexto_comparacion_actual:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay comparación activa. Ejecute primero 'Comparar IA vs calibrador'.",
                parent=self,
            )
            return
        resultados = self._get_estimacion_resultados_muestra_actual()
        if not resultados:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay resultados de estimación IA para guardar.",
                parent=self,
            )
            return

        rows: list[dict[str, Any]] = []
        incompletas = 0
        for id_foto, estimacion in resultados.items():
            if estimacion.get("error") or estimacion.get("apta_para_estimacion") != "Sí":
                incompletas += 1
                continue
            distribucion = estimacion.get("distribucion", [])
            if not isinstance(distribucion, list) or not distribucion:
                incompletas += 1
                continue
            rows.append(self._build_historial_row(id_foto, estimacion))

        if not rows:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay filas completas para guardar. Revise estimación IA y comparación.",
                parent=self,
            )
            return

        self._set_controles_lote_ia_habilitados(False)
        self.estado_var.set(f"Guardando histórico IA ({len(rows)} filas)...")

        def worker() -> None:
            try:
                saved = self.history_repo.save_comparison(rows)
                self.after(0, lambda: self._on_guardar_historico_ok(saved, incompletas))
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: self._on_guardar_historico_error(error))

        threading.Thread(target=worker, daemon=True).start()

    def _abrir_panel_historico_ia(self) -> None:
        try:
            if self._history_window and self._history_window.winfo_exists():
                self._history_window.focus_set()
                self._history_window.lift()
                return
            self._history_window = CalibresIAHistoryWindow(self, self.history_repo)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Obtención calibres",
                f"No se pudo abrir el panel histórico IA:\n{exc}",
                parent=self,
            )

    def _on_guardar_historico_ok(self, saved_rows: int, skipped_rows: int) -> None:
        self._set_controles_lote_ia_habilitados(True)
        ruta = self.history_repo.db_path
        self.estado_var.set(f"Histórico IA guardado ({saved_rows} filas).")
        sufijo = f"\nFilas omitidas por incompletas: {skipped_rows}." if skipped_rows else ""
        messagebox.showinfo(
            "Obtención calibres",
            f"Comparación histórica guardada.\nFilas persistidas: {saved_rows}.\nDB: {ruta}{sufijo}",
            parent=self,
        )

    def _on_guardar_historico_error(self, exc: Exception) -> None:
        self._set_controles_lote_ia_habilitados(True)
        self.estado_var.set("Error al guardar histórico IA.")
        messagebox.showerror(
            "Obtención calibres",
            f"No se pudo guardar histórico IA en {self.history_repo.db_path}: {exc}",
            parent=self,
        )

    def _pintar_resultados_ia(self) -> None:
        self._limpiar_resultados_ia()
        resultados = self._get_ia_resultados_muestra_actual()
        if not resultados:
            return

        aptas = 0
        no_aptas = 0
        errores = 0
        conf_values: list[float] = []

        for id_foto in sorted(resultados.keys()):
            row = resultados[id_foto]
            if row.get("error"):
                errores += 1
            elif row.get("apta") == "Sí":
                aptas += 1
            else:
                no_aptas += 1

            conf = self._confianza_a_float(row.get("confianza"))
            if conf is not None:
                conf_values.append(conf)

            self.tree_validacion_ia.insert(
                "",
                "end",
                iid=id_foto,
                values=(
                    id_foto,
                    row.get("apta", "-"),
                    row.get("confianza", "-"),
                    row.get("oclusion", "-"),
                    row.get("patron_visible", "-"),
                    row.get("estado", "-"),
                ),
            )

        total = len(resultados)
        confianza_media = f"{(sum(conf_values) / len(conf_values)):.2f}" if conf_values else "-"
        self.resumen_ia_lote_var.set(
            f"IA lote: evaluadas={total} | aptas={aptas} | no aptas={no_aptas} | errores={errores} | confianza media={confianza_media}"
        )
        self._actualizar_resumen_global()

    def _set_controles_lote_ia_habilitados(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for btn in (
            self.btn_seleccionar_todas,
            self.btn_deseleccionar_todas,
            self.btn_invertir_seleccion,
            self.btn_detectar_patron,
            self.btn_analizar_frutos,
            self.btn_validacion_ia,
            self.btn_validar_lote_ia,
            self.btn_usar_solo_aptas_ia,
            self.btn_estimacion_calibres_ia,
            self.btn_ver_detalle_estimacion_ia,
            self.btn_comparar_calibrador,
            self.btn_guardar_historico,
            self.btn_ver_historico_ia,
            self.btn_preparar_analisis,
            self.btn_ejecutar_flujo,
        ):
            btn.config(state=state)

    @staticmethod
    def _build_distribucion_texto(distribucion: list[dict[str, Any]]) -> str:
        if not distribucion:
            return "-"
        partes = [f"{item['calibre']}:{item['porcentaje']:.1f}%" for item in distribucion if item.get("calibre")]
        return " | ".join(partes) if partes else "-"

    def _pintar_resultados_estimacion_ia(self) -> None:
        self._limpiar_resultados_estimacion_ia()
        resultados = self._get_estimacion_resultados_muestra_actual()
        if not resultados:
            return

        aptas = 0
        conf_values: list[float] = []
        advertencias: list[str] = []
        consolidado: dict[str, float] = {}
        count_distribucion = 0

        for id_foto in sorted(resultados.keys()):
            row = resultados[id_foto]
            if row.get("error"):
                estado = row.get("estado", "Error")
            else:
                estado = row.get("estado", "OK")
                if row.get("apta_para_estimacion") == "Sí":
                    aptas += 1
                    for item in row.get("distribucion", []):
                        calibre = str(item.get("calibre", "")).strip()
                        pct = self._confianza_a_float(item.get("porcentaje"))
                        if calibre and pct is not None:
                            consolidado[calibre] = consolidado.get(calibre, 0.0) + pct
                    count_distribucion += 1
                for advertencia in row.get("advertencias", []):
                    texto = str(advertencia).strip()
                    if texto:
                        advertencias.append(texto)
                diagnostico = str(row.get("diagnostico", "")).strip()
                if diagnostico:
                    advertencias.append(diagnostico)

            conf = self._confianza_a_float(row.get("confianza"))
            if conf is not None:
                conf_values.append(conf)

            self.tree_estimacion_ia.insert(
                "",
                "end",
                iid=f"est_{id_foto}",
                values=(
                    id_foto,
                    row.get("apta_para_estimacion", "-"),
                    row.get("confianza", "-"),
                    row.get("frutos_visibles_estimados", "-"),
                    row.get("calibre_dominante", "-"),
                    self._build_distribucion_texto(row.get("distribucion", [])),
                    estado if not row.get("error") else row.get("estado", "Error"),
                ),
            )

        confianza_media = f"{(sum(conf_values) / len(conf_values)):.2f}" if conf_values else "-"
        consolidado_texto = "-"
        if consolidado and count_distribucion > 0:
            promedio = {k: v / count_distribucion for k, v in consolidado.items()}
            total_prom = sum(promedio.values())
            if total_prom > 0:
                norm = {k: (v * 100.0 / total_prom) for k, v in promedio.items()}
                partes = [f"{k}:{norm[k]:.1f}%" for k in sorted(norm.keys())]
                consolidado_texto = " | ".join(partes)
        self.resumen_estimacion_ia_var.set(
            "Estimación IA experimental: "
            f"evaluadas={len(resultados)} | aptas={aptas} | confianza media={confianza_media} | "
            f"distribución consolidada={consolidado_texto}"
        )
        advertencias_unicas = []
        for adv in advertencias:
            if adv not in advertencias_unicas:
                advertencias_unicas.append(adv)
        self.advertencias_estimacion_ia_var.set(
            "Advertencias estimación IA experimental: "
            + (" | ".join(advertencias_unicas[:5]) if advertencias_unicas else "-")
        )
        self._actualizar_resumen_global()

    def _ejecutar_estimacion_calibres_ia(self) -> None:
        if self._ai_estimacion_en_curso or self._ai_lote_en_curso or self._ai_validacion_en_curso:
            return
        if not self._current_muestra_id:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para estimación IA experimental.", parent=self)
            return

        ids_seleccionadas = set(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "No hay fotos seleccionadas.", parent=self)
            return

        resultados_ia = self._get_ia_resultados_muestra_actual()
        ids_aptas_ia = {
            id_foto
            for id_foto in ids_seleccionadas
            if resultados_ia.get(id_foto) and not resultados_ia[id_foto].get("error") and resultados_ia[id_foto].get("apta") == "Sí"
        }
        if not ids_aptas_ia:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay fotos aptas IA en la selección actual. Ejecute primero 'Validar lote IA'.",
                parent=self,
            )
            return

        ids_candidatas = set(ids_aptas_ia)
        if self._deteccion_resultados:
            ids_candidatas = {
                id_foto
                for id_foto in ids_candidatas
                if self._deteccion_resultados.get(id_foto) and self._deteccion_resultados[id_foto].valid_for_next_step
            }
            if not ids_candidatas:
                messagebox.showwarning(
                    "Obtención calibres",
                    "Existe resultado de patrón, pero ninguna foto apta IA tiene patrón válido.",
                    parent=self,
                )
                return

        cards_by_id = {str(card.get("foto", {}).get("id_foto", "")): card for card in self._current_cards}
        muestra = next((item for item in self._muestras if item["id_muestra"] == self._current_muestra_id), None)
        cultivo = str(muestra.get("cultivo", "")).strip() if muestra else ""
        cultivo_payload = cultivo.strip().upper() or "*"
        variedad = self._resolver_variedad_ia()
        rangos = self._config.rangos_por_cultivo.get(cultivo, []) if self._config else []
        diametro_patron = self._config.diametro_patron_mm if self._config else 94.0
        if not rangos:
            messagebox.showwarning(
                "Obtención calibres",
                f"No hay rangos de calibres configurados para cultivo '{cultivo or '-'}'.",
                parent=self,
            )
            return
        service_url, _source, resolve_error = self.data_service.resolve_url_servicio_ia()
        if not service_url:
            messagebox.showerror("Obtención calibres", f"No hay URL de servicio IA: {resolve_error or '-'}", parent=self)
            return

        self._ai_estimacion_en_curso = True
        self._set_controles_lote_ia_habilitados(False)
        self.estado_var.set(f"Estimación IA experimental 0/{len(ids_candidatas)}...")
        resultados_estimacion = self._get_estimacion_resultados_muestra_actual()
        resultados_estimacion.clear()

        contexto_base = {
            "tipo_tarea": "estimacion_calibres_experimental",
            "cultivo": cultivo,
            "variedad": variedad,
            "diametro_patron_mm": diametro_patron,
            "rangos_calibres": rangos,
            "respuesta_esperada": {
                "formato": "json_estricto",
                "campos_minimos": [
                    "apta_para_estimacion",
                    "confianza",
                    "frutos_visibles_estimados",
                    "calibre_dominante",
                    "distribucion",
                ],
            },
            "nota": (
                "Estimación IA experimental para distribución aproximada por foto. "
                "Devuelve únicamente JSON estricto sin markdown."
            ),
        }

        def worker() -> None:
            for idx, id_foto in enumerate(sorted(ids_candidatas), start=1):
                row: dict[str, Any] = {
                    "apta_para_estimacion": "-",
                    "confianza": "-",
                    "frutos_visibles_estimados": "-",
                    "calibre_dominante": "-",
                    "distribucion": [],
                    "advertencias": [],
                    "resumen": "-",
                    "estado": "",
                    "error": True,
                }
                try:
                    card = cards_by_id.get(id_foto)
                    if not card:
                        raise ValueError("Foto no cargada en memoria.")
                    ruta_local = str(card.get("foto", {}).get("ruta_local", "")).strip()
                    image_url_for_ai = self._build_image_url_for_ai(ruta_local)
                    if not image_url_for_ai:
                        raise ValueError("No se pudo construir image_url.")
                    contexto = dict(contexto_base)
                    contexto["id_foto"] = id_foto
                    contexto["image_url"] = image_url_for_ai
                    LOGGER.info(
                        "Estimación IA request: id_foto=%s image_url=%s task=%s cultivo=%s rangos=%s",
                        id_foto,
                        image_url_for_ai,
                        "estimacion_calibres",
                        cultivo,
                        len(rangos),
                    )
                    result = call_analyze_image(
                        server_url=service_url,
                        image_url=image_url_for_ai,
                        task="estimacion_calibres",
                        context=json.dumps(contexto, ensure_ascii=False),
                        cultivo=cultivo_payload,
                        variedad=variedad,
                        timeout_seconds=30,
                    )
                    parsed = self._parse_estimacion_ia_result(result)
                    row.update(parsed)
                    row["task_enviada"] = "estimacion_calibres"
                    row["image_url"] = image_url_for_ai
                    row["id_foto"] = id_foto
                    row["error"] = not bool(parsed.get("es_valida"))
                    if row["error"]:
                        error_tipo = str(parsed.get("error_tipo", "")).strip()
                        if error_tipo == "parse":
                            row["estado"] = "JSON IA no parseable"
                        elif error_tipo == "campos":
                            row["estado"] = "Respuesta IA sin campos esperados"
                        else:
                            row["estado"] = parsed.get("diagnostico") or "Respuesta IA sin campos esperados"
                    else:
                        row["estado"] = "OK"
                    row["raw_result"] = result
                except Exception as exc:  # noqa: BLE001
                    row["task_enviada"] = "estimacion_calibres"
                    row["estado"] = "Error servicio IA"
                    row["diagnostico"] = f"Error servicio IA: {exc}"
                    row["error"] = True
                finally:
                    resultados_estimacion[id_foto] = row
                    self.after(0, lambda i=idx: self.estado_var.set(f"Estimación IA experimental {i}/{len(ids_candidatas)}..."))
                    self.after(0, self._pintar_resultados_estimacion_ia)

            self.after(0, self._on_estimacion_ia_finalizada)

        threading.Thread(target=worker, daemon=True).start()

    def _on_estimacion_ia_finalizada(self) -> None:
        self._ai_estimacion_en_curso = False
        self._set_controles_lote_ia_habilitados(True)
        self._pintar_resultados_estimacion_ia()
        self.estado_var.set("Estimación IA experimental finalizada.")

    def _set_estado_paso_flujo(self, step_number: int, total_steps: int, text: str) -> None:
        self.estado_var.set(f"Paso {step_number}/{total_steps}: {text}")

    def _tiene_validacion_ia_completa(self, ids_foto: set[str]) -> bool:
        if not ids_foto:
            return False
        resultados = self._get_ia_resultados_muestra_actual()
        for id_foto in ids_foto:
            row = resultados.get(id_foto)
            if not row or row.get("error"):
                return False
            if row.get("apta") not in ("Sí", "No"):
                return False
        return True

    def _tiene_patron_valido_completo(self, ids_foto: set[str]) -> bool:
        if not ids_foto:
            return False
        return all(
            self._deteccion_resultados.get(id_foto) and self._deteccion_resultados[id_foto].valid_for_next_step
            for id_foto in ids_foto
        )

    def _tiene_analisis_frutos_completo(self, ids_foto: set[str]) -> bool:
        if not ids_foto:
            return False
        return all(self._frutos_resultados.get(id_foto) is not None for id_foto in ids_foto)

    def _ejecutar_flujo_recomendado(self) -> None:
        if self._flujo_recomendado_en_curso or self._ai_lote_en_curso or self._ai_validacion_en_curso:
            return
        if not self._current_muestra_id:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para ejecutar el flujo recomendado.", parent=self)
            return

        ids_seleccionadas = set(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "Faltan fotos seleccionadas para iniciar el flujo.", parent=self)
            return

        cards_by_id = {str(card.get("foto", {}).get("id_foto", "")): card for card in self._current_cards}
        if any(id_foto not in cards_by_id for id_foto in ids_seleccionadas):
            messagebox.showwarning("Obtención calibres", "Hay fotos seleccionadas sin descargar. Recargue la muestra.", parent=self)
            return
        if self._detector is None:
            messagebox.showerror("Obtención calibres", "No hay configuración cargada para detección de patrón.", parent=self)
            return

        service_url, source, resolve_error = self.data_service.resolve_url_servicio_ia()
        if not service_url and not self._tiene_validacion_ia_completa(ids_seleccionadas):
            messagebox.showerror(
                "Obtención calibres",
                (
                    "No hay URL de servicio IA para ejecutar validación de lote.\n"
                    "Orden de resolución: HARVESTSYNC_INTERNAL_AI_URL -> ServidorIA/url_actual/url.\n"
                    f"Detalle: {resolve_error or 'No hay configuración disponible.'}"
                ),
                parent=self,
            )
            return
        LOGGER.info("Flujo recomendado: inicio muestra=%s source_ia=%s", self._current_muestra_id, source)
        cultivo_ia = self._resolver_cultivo_ia()
        variedad_ia = self._resolver_variedad_ia()

        self._flujo_recomendado_en_curso = True
        self._set_controles_lote_ia_habilitados(False)
        total_steps = 5

        def worker() -> None:
            try:
                id_muestra = self._current_muestra_id or ""
                selected_ids = set(self._selected_fotos_by_muestra.get(id_muestra, set()))
                if not self._tiene_validacion_ia_completa(selected_ids):
                    self.after(0, lambda: self._set_estado_paso_flujo(1, total_steps, "Validando IA por lote..."))
                    resultados_ia = self._get_ia_resultados_muestra_actual()
                    for id_foto in sorted(selected_ids):
                        card = cards_by_id.get(id_foto)
                        if not card:
                            continue
                        ruta_local = str(card.get("foto", {}).get("ruta_local", "")).strip()
                        image_url_for_ai = self._build_image_url_for_ai(ruta_local)
                        row: dict[str, Any] = {
                            "apta": "-",
                            "confianza": "-",
                            "oclusion": "-",
                            "patron_visible": "-",
                            "estado": "",
                            "error": True,
                            "image_url": image_url_for_ai,
                        }
                        try:
                            result = call_analyze_image(
                                server_url=service_url,
                                image_url=image_url_for_ai,
                                task="validacion_foto",
                                context=(
                                    "Evaluar utilidad de imagen para calibres: "
                                    "visibilidad general, oclusión, nitidez y presencia/claridad del patrón."
                                ),
                                cultivo=cultivo_ia,
                                variedad=variedad_ia,
                                timeout_seconds=25,
                            )
                            parsed = self._parse_validacion_ia_result(result)
                            row.update(
                                {
                                    "apta": parsed.get("apta", "-"),
                                    "confianza": parsed.get("confianza", "-"),
                                    "oclusion": parsed.get("oclusion", "-"),
                                    "patron_visible": parsed.get("patron_visible", "-"),
                                    "estado": "OK",
                                    "error": False,
                                    "raw_result": result,
                                    "parsed": parsed,
                                }
                            )
                        except Exception as exc:  # noqa: BLE001
                            row["estado"] = str(exc)
                            row["error"] = True
                        resultados_ia[id_foto] = row
                else:
                    self.after(0, lambda: self._set_estado_paso_flujo(1, total_steps, "Validación IA ya disponible, se omite."))

                self.after(0, self._pintar_resultados_ia)

                self.after(0, lambda: self._set_estado_paso_flujo(2, total_steps, "Aplicando fotos aptas IA..."))
                resultados_ia = self._get_ia_resultados_muestra_actual()
                aptas_ids = {
                    id_foto
                    for id_foto in selected_ids
                    if resultados_ia.get(id_foto) and not resultados_ia[id_foto].get("error") and resultados_ia[id_foto].get("apta") == "Sí"
                }
                if not aptas_ids:
                    self.after(
                        0,
                        lambda: self._on_flujo_recomendado_error(
                            "La validación IA no dejó fotos aptas. Ajuste capturas o revise el lote."
                        ),
                    )
                    return
                self._selected_fotos_by_muestra[id_muestra] = aptas_ids
                selected_ids = set(aptas_ids)

                if not self._tiene_patron_valido_completo(selected_ids):
                    self.after(0, lambda: self._set_estado_paso_flujo(3, total_steps, "Detectando patrón..."))
                    resultados_patron: dict[str, CircleDetectionResult] = {}
                    overlays_patron: dict[str, str] = {}
                    for id_foto in sorted(selected_ids):
                        card = cards_by_id.get(id_foto)
                        if not card:
                            continue
                        result = self._detector.detect_from_bytes(id_foto, card.get("raw") or b"")
                        resultados_patron[id_foto] = result
                        overlay_path = self._save_overlay_image(id_foto, card.get("raw") or b"", result)
                        if overlay_path:
                            overlays_patron[id_foto] = overlay_path
                    self._deteccion_resultados = resultados_patron
                    self._overlay_paths_by_foto = overlays_patron
                else:
                    self.after(0, lambda: self._set_estado_paso_flujo(3, total_steps, "Patrón ya detectado, se omite."))

                patrones_validos = [
                    id_foto
                    for id_foto in selected_ids
                    if self._deteccion_resultados.get(id_foto) and self._deteccion_resultados[id_foto].valid_for_next_step
                ]
                if not patrones_validos:
                    self.after(
                        0,
                        lambda: self._on_flujo_recomendado_error(
                            "No hay patrón válido en las fotos aptas. Deteniendo flujo recomendado."
                        ),
                    )
                    return

                if not self._tiene_analisis_frutos_completo(selected_ids):
                    self.after(0, lambda: self._set_estado_paso_flujo(4, total_steps, "Analizando frutos..."))
                    muestra = next((item for item in self._muestras if item["id_muestra"] == id_muestra), None)
                    cultivo = str(muestra.get("cultivo", "")).strip() if muestra else ""
                    rangos = self._config.rangos_por_cultivo.get(cultivo, []) if self._config else []
                    resultados_frutos: dict[str, PhotoFruitAnalysisResult] = {}
                    overlays_frutos: dict[str, str] = {}
                    for id_foto in sorted(selected_ids):
                        escala = self._deteccion_resultados.get(id_foto)
                        card = cards_by_id.get(id_foto)
                        if not escala or not escala.valid_for_next_step or escala.mm_per_pixel is None or not card:
                            continue
                        result = self._fruit_analyzer.analyze_photo(
                            image_id=id_foto,
                            raw_image=card.get("raw") or b"",
                            mm_per_pixel=escala.mm_per_pixel,
                            caliber_ranges=rangos,
                        )
                        resultados_frutos[id_foto] = result
                        overlay = self._save_fruit_overlay_image(id_foto, card.get("raw") or b"", result)
                        if overlay:
                            overlays_frutos[id_foto] = overlay
                    self._frutos_resultados = resultados_frutos
                    self._frutos_overlay_paths_by_foto = overlays_frutos
                else:
                    self.after(0, lambda: self._set_estado_paso_flujo(4, total_steps, "Análisis de frutos ya disponible, se omite."))

                self.after(0, lambda: self._set_estado_paso_flujo(5, total_steps, "Preparando análisis final..."))
                self.after(0, lambda resultados=self._deteccion_resultados: self._pintar_resultados_deteccion(resultados))
                self.after(0, self._pintar_resultados_frutos)
                self.after(0, self._render_cards, self._current_cards)
                self.after(0, self._finalizar_flujo_recomendado)
            except Exception as exc:  # noqa: BLE001
                self.after(0, lambda error=exc: self._on_flujo_recomendado_error(f"Error en flujo recomendado: {error}"))

        threading.Thread(target=worker, daemon=True).start()

    def _finalizar_flujo_recomendado(self) -> None:
        ok, detalle = self._preparar_analisis_interno(show_message=False)
        self._flujo_recomendado_en_curso = False
        self._set_controles_lote_ia_habilitados(True)
        self._actualizar_resumen_global()
        if not ok:
            messagebox.showwarning("Obtención calibres", detalle, parent=self)
            return
        frutos_validos = 0
        seleccionadas = self._selected_fotos_by_muestra.get(self._current_muestra_id or "", set())
        for id_foto in seleccionadas:
            res = self._frutos_resultados.get(id_foto)
            if res:
                frutos_validos += len([item for item in res.fruits if item.valid])
        if frutos_validos == 0:
            messagebox.showwarning(
                "Obtención calibres",
                "Flujo completado, pero no se detectaron frutos válidos. Revise captura o parámetros operativos.",
                parent=self,
            )
        else:
            messagebox.showinfo("Obtención calibres", "Flujo recomendado completado correctamente.", parent=self)
        self.estado_var.set("Flujo recomendado finalizado.")

    def _on_flujo_recomendado_error(self, mensaje: str) -> None:
        self._flujo_recomendado_en_curso = False
        self._set_controles_lote_ia_habilitados(True)
        self._actualizar_resumen_global()
        self.estado_var.set("Flujo recomendado detenido.")
        messagebox.showwarning("Obtención calibres", mensaje, parent=self)

    def _validar_lote_ia(self) -> None:
        if self._ai_lote_en_curso:
            return
        if not self._current_muestra_id:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para validar lote IA.", parent=self)
            return

        ids_seleccionadas = sorted(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        if not ids_seleccionadas:
            messagebox.showwarning("Obtención calibres", "No hay fotos marcadas en 'Usar en análisis'.", parent=self)
            return

        cards_by_id = {str(card.get("foto", {}).get("id_foto", "")): card for card in self._current_cards}
        service_url, source, resolve_error = self.data_service.resolve_url_servicio_ia()
        if not service_url:
            messagebox.showerror(
                "Obtención calibres",
                (
                    "No hay URL de servicio IA.\n"
                    "Orden de resolución: HARVESTSYNC_INTERNAL_AI_URL -> ServidorIA/url_actual/url.\n"
                    f"Detalle: {resolve_error or 'No hay configuración disponible.'}"
                ),
                parent=self,
            )
            return

        LOGGER.info("Validación IA lote: inicio. muestra=%s fotos=%s source=%s", self._current_muestra_id, len(ids_seleccionadas), source)
        timeout_seconds = 25
        self._ai_lote_en_curso = True
        self._set_controles_lote_ia_habilitados(False)
        self.estado_var.set(f"Validando IA 0/{len(ids_seleccionadas)}...")
        resultados_lote = self._get_ia_resultados_muestra_actual()
        cultivo_ia = self._resolver_cultivo_ia()
        variedad_ia = self._resolver_variedad_ia()

        def worker() -> None:
            batch_t0 = time.perf_counter()
            for idx, id_foto in enumerate(ids_seleccionadas, start=1):
                foto_t0 = time.perf_counter()
                LOGGER.info("Validación IA lote: foto %s/%s id_foto=%s", idx, len(ids_seleccionadas), id_foto)
                row: dict[str, Any] = {
                    "apta": "-",
                    "confianza": "-",
                    "oclusion": "-",
                    "patron_visible": "-",
                    "estado": "",
                    "error": True,
                    "image_url": "",
                }
                try:
                    card = cards_by_id.get(id_foto)
                    if not card:
                        raise ValueError("La foto no está cargada en memoria para procesar.")
                    ruta_local = str(card.get("foto", {}).get("ruta_local", "")).strip()
                    if not ruta_local:
                        raise ValueError("No existe 'ruta_local' en la foto.")
                    image_url_for_ai = self._build_image_url_for_ai(ruta_local)
                    if not image_url_for_ai:
                        raise ValueError("No se pudo construir image_url HTTP para la foto.")
                    row["image_url"] = image_url_for_ai
                    result = call_analyze_image(
                        server_url=service_url,
                        image_url=image_url_for_ai,
                        task="validacion_foto",
                        context=(
                            "Evaluar utilidad de imagen para calibres: "
                            "visibilidad general, oclusión, nitidez y presencia/claridad del patrón."
                        ),
                        cultivo=cultivo_ia,
                        variedad=variedad_ia,
                        timeout_seconds=timeout_seconds,
                    )
                    parsed = self._parse_validacion_ia_result(result)
                    row.update(
                        {
                            "apta": parsed.get("apta", "-"),
                            "confianza": parsed.get("confianza", "-"),
                            "oclusion": parsed.get("oclusion", "-"),
                            "patron_visible": parsed.get("patron_visible", "-"),
                            "estado": "OK",
                            "error": False,
                            "raw_result": result,
                            "parsed": parsed,
                        }
                    )
                    LOGGER.info(
                        "Validación IA lote: resultado OK id_foto=%s apta=%s confianza=%s",
                        id_foto,
                        row["apta"],
                        row["confianza"],
                    )
                except Exception as exc:  # noqa: BLE001
                    row["estado"] = str(exc)
                    row["error"] = True
                    LOGGER.error("Validación IA lote: error id_foto=%s error=%s", id_foto, exc)
                finally:
                    row["duracion_s"] = time.perf_counter() - foto_t0
                    resultados_lote[id_foto] = row
                    self.after(0, lambda i=idx: self.estado_var.set(f"Validando IA {i}/{len(ids_seleccionadas)}..."))
                    self.after(0, self._pintar_resultados_ia)

            total_elapsed = time.perf_counter() - batch_t0
            LOGGER.info("Validación IA lote: fin. muestra=%s duracion=%.2fs", self._current_muestra_id, total_elapsed)
            self.after(0, lambda elapsed=total_elapsed: self._on_lote_ia_finalizado(elapsed))

        threading.Thread(target=worker, daemon=True).start()

    def _on_lote_ia_finalizado(self, elapsed: float) -> None:
        self._ai_lote_en_curso = False
        self._set_controles_lote_ia_habilitados(True)
        self._pintar_resultados_ia()
        self._render_cards(self._current_cards)

        resultados = self._get_ia_resultados_muestra_actual()
        aptas = sum(1 for row in resultados.values() if not row.get("error") and row.get("apta") == "Sí")
        errores = sum(1 for row in resultados.values() if row.get("error"))
        no_aptas = max(len(resultados) - aptas - errores, 0)
        conf_values = [self._confianza_a_float(row.get("confianza")) for row in resultados.values()]
        conf_validas = [item for item in conf_values if item is not None]
        conf_media = f"{(sum(conf_validas) / len(conf_validas)):.2f}" if conf_validas else "-"
        self.estado_var.set(f"Validación IA lote finalizada: {len(resultados)} foto(s) en {elapsed:.1f}s.")
        messagebox.showinfo(
            "Obtención calibres - Validación IA por lote",
            (
                "Resumen validación IA por lote\n\n"
                f"Fotos evaluadas: {len(resultados)}\n"
                f"Aptas: {aptas}\n"
                f"No aptas: {no_aptas}\n"
                f"Errores: {errores}\n"
                f"Confianza media: {conf_media}\n"
                f"Duración total: {elapsed:.1f}s"
            ),
            parent=self,
        )

    def _usar_solo_aptas_ia(self) -> None:
        if self._ai_lote_en_curso:
            return
        if not self._current_muestra_id:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra.", parent=self)
            return

        resultados = self._get_ia_resultados_muestra_actual()
        if not resultados:
            messagebox.showinfo("Obtención calibres", "No hay resultados IA por lote para aplicar.", parent=self)
            return

        actuales = self._selected_fotos_by_muestra.get(self._current_muestra_id, set())
        nuevas = {
            id_foto
            for id_foto in actuales
            if resultados.get(id_foto) and not resultados[id_foto].get("error") and resultados[id_foto].get("apta") == "Sí"
        }
        self._selected_fotos_by_muestra[self._current_muestra_id] = nuevas
        self._analysis_payload = {}
        self._render_cards(self._current_cards)
        self.estado_var.set(f"Filtro IA aplicado: {len(nuevas)} foto(s) aptas seleccionadas.")

    def _mostrar_resultado_ia(self, id_foto: str, image_ref: str, result: dict[str, Any]) -> None:
        win = tk.Toplevel(self)
        win.title(f"Resultado Validación IA - {id_foto}")
        win.geometry("860x620")
        win.minsize(700, 500)

        cont = ttk.Frame(win, padding=10)
        cont.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        cont.rowconfigure(2, weight=1)
        cont.columnconfigure(0, weight=1)
        parsed = self._parse_validacion_ia_result(result)

        ttk.Label(cont, text=f"Foto: {id_foto}", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(cont, text=f"URL enviada: {image_ref}", foreground="#1b4f72", wraplength=820).grid(
            row=1,
            column=0,
            sticky="w",
            pady=(2, 8),
        )

        resumen_frame = ttk.LabelFrame(cont, text="Validación IA (operativa)")
        resumen_frame.grid(row=2, column=0, sticky="nsew")
        resumen_frame.columnconfigure(1, weight=1)

        filas = [
            ("Apta", parsed["apta"]),
            ("Confianza", parsed["confianza"]),
            ("Oclusión", parsed["oclusion"]),
            ("Patrón visible", parsed["patron_visible"]),
            ("Box centrado", parsed["box_centrado"]),
            ("Resumen", parsed["resumen"]),
            ("Recomendación", parsed["recomendacion"]),
            ("Modelo", parsed["modelo"]),
            ("raw_id", parsed["raw_id"]),
        ]
        for idx, (label, value) in enumerate(filas):
            ttk.Label(resumen_frame, text=f"{label}:", font=("Segoe UI", 9, "bold")).grid(
                row=idx,
                column=0,
                sticky="nw",
                padx=(8, 6),
                pady=3,
            )
            ttk.Label(resumen_frame, text=str(value), wraplength=620, justify="left").grid(
                row=idx,
                column=1,
                sticky="w",
                padx=(0, 8),
                pady=3,
            )

        alertas = parsed["alertas"]
        alertas_texto = "\n".join(f"• {item}" for item in alertas) if alertas else "Sin alertas"
        row_alertas = len(filas)
        ttk.Label(resumen_frame, text="Alertas:", font=("Segoe UI", 9, "bold")).grid(
            row=row_alertas,
            column=0,
            sticky="nw",
            padx=(8, 6),
            pady=3,
        )
        ttk.Label(resumen_frame, text=alertas_texto, wraplength=620, justify="left").grid(
            row=row_alertas,
            column=1,
            sticky="w",
            padx=(0, 8),
            pady=3,
        )

        bruto_frame = ttk.LabelFrame(cont, text="JSON bruto")
        bruto_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        bruto_frame.rowconfigure(0, weight=1)
        bruto_frame.columnconfigure(0, weight=1)
        cont.rowconfigure(3, weight=1)

        text = tk.Text(bruto_frame, wrap="word", height=10)
        text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(bruto_frame, orient="vertical", command=text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        text.configure(yscrollcommand=scroll.set)

        if parsed["json_parse_ok"]:
            raw_pretty = json.dumps(parsed["json_parseado"], ensure_ascii=False, indent=2)
        else:
            raw_pretty = str(parsed["output_text_original"])
        payload_pretty = json.dumps(result, ensure_ascii=False, indent=2)
        text.insert("1.0", f"output_text (original o parseado):\n{raw_pretty}\n\npayload completo:\n{payload_pretty}")
        text.configure(state="disabled")

    def _on_double_click_estimacion_ia(self, _event: tk.Event) -> None:
        self._ver_detalle_estimacion_ia_seleccionada()

    def _ver_detalle_estimacion_ia_seleccionada(self) -> None:
        selected = self.tree_estimacion_ia.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione una fila de estimación IA.", parent=self)
            return
        iid = selected[0]
        id_foto = iid[4:] if iid.startswith("est_") else iid
        resultados = self._get_estimacion_resultados_muestra_actual()
        row = resultados.get(id_foto)
        if not row:
            messagebox.showwarning("Obtención calibres", "No hay detalle IA para la fila seleccionada.", parent=self)
            return
        self._mostrar_detalle_estimacion_ia(id_foto=id_foto, row=row)

    def _mostrar_detalle_estimacion_ia(self, id_foto: str, row: dict[str, Any]) -> None:
        win = tk.Toplevel(self)
        win.title(f"Detalle Estimación IA - {id_foto}")
        win.geometry("900x620")
        win.minsize(760, 500)

        cont = ttk.Frame(win, padding=10)
        cont.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        cont.rowconfigure(1, weight=1)
        cont.columnconfigure(0, weight=1)

        task_enviada = row.get("task_enviada", "estimacion_calibres")
        image_url = str(row.get("image_url", "-"))
        output_text = str(row.get("output_text_original", ""))
        parseado = row.get("json_parseado")
        parseado_pretty = json.dumps(parseado, ensure_ascii=False, indent=2) if isinstance(parseado, dict) else "-"
        diagnostico = str(row.get("diagnostico", "")).strip() or "-"
        raw_result = row.get("raw_result", {})
        modelo_ia = str(raw_result.get("model", "") or "").strip() or "-"
        prompt_version = str(raw_result.get("prompt_version", "") or "").strip() or "-"
        prompt_source = str(raw_result.get("prompt_source", "") or "").strip() or "no_informado"
        cultivo = str(raw_result.get("cultivo", "") or "").strip() or "-"
        variedad = str(raw_result.get("variedad", "") or "").strip() or "-"

        encabezado = (
            f"id_foto: {id_foto}\n"
            f"image_url: {image_url}\n"
            f"task: {task_enviada}\n"
            f"modelo: {modelo_ia}\n"
            f"prompt_version: {prompt_version}\n"
            f"prompt_source: {prompt_source}\n"
            f"cultivo: {cultivo}\n"
            f"variedad: {variedad}\n"
            f"estado: {row.get('estado', '-')}\n"
            f"diagnóstico: {diagnostico}"
        )
        ttk.Label(cont, text=encabezado, justify="left", wraplength=860).grid(row=0, column=0, sticky="w")

        frame_text = ttk.LabelFrame(cont, text="Detalle respuesta IA")
        frame_text.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        frame_text.rowconfigure(0, weight=1)
        frame_text.columnconfigure(0, weight=1)

        text = tk.Text(frame_text, wrap="word")
        text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(frame_text, orient="vertical", command=text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        text.configure(yscrollcommand=scroll.set)
        text.insert(
            "1.0",
            f"output_text bruto:\n{output_text or '-'}\n\n"
            f"json_parseado:\n{parseado_pretty}\n\n"
            f"payload result completo:\n{json.dumps(row.get('raw_result', {}), ensure_ascii=False, indent=2)}",
        )
        text.configure(state="disabled")

    def _on_toggle_foto(self, id_foto: str, usar_en_analisis: bool) -> None:
        if not self._current_muestra_id or not id_foto:
            return
        seleccionadas = self._selected_fotos_by_muestra.setdefault(self._current_muestra_id, set())
        if usar_en_analisis:
            seleccionadas.add(id_foto)
        else:
            seleccionadas.discard(id_foto)
        self._analysis_payload = {}
        self._deteccion_resultados.pop(id_foto, None)
        self._frutos_resultados.pop(id_foto, None)
        self._frutos_overlay_paths_by_foto.pop(id_foto, None)
        self._ia_validacion_resultados_by_muestra.setdefault(self._current_muestra_id, {}).pop(id_foto, None)
        self._ia_estimacion_resultados_by_muestra.setdefault(self._current_muestra_id, {}).pop(id_foto, None)
        self._pintar_resultados_frutos()
        self._pintar_resultados_ia()
        self._pintar_resultados_estimacion_ia()
        self._actualizar_resumen_fotos()

    def _actualizar_resumen_global(self) -> None:
        if not hasattr(self, "resumen_global_var"):
            return
        if not self._current_muestra_id:
            self.resumen_global_var.set(
                "Fotos encontradas: 0 | Fotos seleccionadas: 0 | Fotos aptas IA: 0 | Fotos patrón válido: 0 | Frutos válidos: 0"
            )
            if hasattr(self, "resumen_fases_var"):
                self.resumen_fases_var.set(
                    "1. Selección: 0/0 fotos\n"
                    "2. IA: pendiente\n"
                    "3. Patrón: pendiente\n"
                    "4. Frutos: pendiente\n"
                    "5. Preparación: pendiente\n"
                    "Estado final: pendiente"
                )
            return
        total_fotos = len(self._fotos_by_muestra.get(self._current_muestra_id, []))
        seleccionadas = self._selected_fotos_by_muestra.get(self._current_muestra_id, set())
        total_seleccionadas = len(seleccionadas)
        resultados_ia = self._get_ia_resultados_muestra_actual()
        aptas_ia = sum(
            1
            for id_foto in seleccionadas
            if resultados_ia.get(id_foto) and not resultados_ia[id_foto].get("error") and resultados_ia[id_foto].get("apta") == "Sí"
        )
        patrones_validos = sum(
            1
            for id_foto in seleccionadas
            if self._deteccion_resultados.get(id_foto) and self._deteccion_resultados[id_foto].valid_for_next_step
        )
        frutos_validos = 0
        fotos_analizadas = 0
        for id_foto in seleccionadas:
            res = self._frutos_resultados.get(id_foto)
            if not res:
                continue
            fotos_analizadas += 1
            frutos_validos += len([item for item in res.fruits if item.valid])
        self.resumen_global_var.set(
            f"Fotos encontradas: {total_fotos} | Fotos seleccionadas: {total_seleccionadas} | "
            f"Fotos aptas IA: {aptas_ia} | Fotos patrón válido: {patrones_validos} | Fotos analizadas: {fotos_analizadas} | "
            f"Frutos válidos: {frutos_validos}"
        )
        resultados_estimacion = self._get_estimacion_resultados_muestra_actual()
        if resultados_estimacion:
            aptas_estimacion = sum(
                1 for row in resultados_estimacion.values() if not row.get("error") and row.get("apta_para_estimacion") == "Sí"
            )
            self.resumen_global_var.set(
                f"{self.resumen_global_var.get()} | Estimación IA experimental apta: {aptas_estimacion}/{len(resultados_estimacion)}"
            )
        if hasattr(self, "resumen_fases_var"):
            ia_estado = "pendiente"
            if resultados_ia:
                evaluadas = sum(1 for id_foto in seleccionadas if id_foto in resultados_ia)
                ia_estado = f"completada, {aptas_ia} aptas ({evaluadas}/{total_seleccionadas} evaluadas)"
            patron_estado = "pendiente" if patrones_validos == 0 else f"{patrones_validos} fotos válidas"
            frutos_estado = "pendiente" if fotos_analizadas == 0 else f"{frutos_validos} frutos válidos"
            preparacion_estado = "lista" if self._analysis_payload.get("id_muestra") == self._current_muestra_id else "pendiente"
            estado_final = "listo" if preparacion_estado == "lista" else ("en proceso" if total_seleccionadas > 0 else "pendiente")
            self.resumen_fases_var.set(
                f"1. Selección: {total_seleccionadas}/{total_fotos} fotos\n"
                f"2. IA: {ia_estado}\n"
                f"3. Patrón: {patron_estado}\n"
                f"4. Frutos: {frutos_estado}\n"
                f"5. Preparación: {preparacion_estado}\n"
                f"Estado final: {estado_final}"
            )

    def _actualizar_resumen_fotos(self) -> None:
        if not self._current_muestra_id:
            self.resumen_fotos_var.set("Fotos encontradas: 0 | Seleccionadas: 0 | Excluidas: 0")
            self._actualizar_resumen_global()
            return
        total = len(self._fotos_by_muestra.get(self._current_muestra_id, []))
        seleccionadas = len(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        excluidas = max(total - seleccionadas, 0)
        self.resumen_fotos_var.set(
            f"Fotos encontradas: {total} | Seleccionadas: {seleccionadas} | Excluidas: {excluidas}"
        )
        self._actualizar_resumen_global()

    def _seleccionar_todas(self) -> None:
        if not self._current_muestra_id:
            return
        fotos = self._fotos_by_muestra.get(self._current_muestra_id, [])
        self._selected_fotos_by_muestra[self._current_muestra_id] = {
            str(foto.get("id_foto", "")) for foto in fotos if foto.get("id_foto")
        }
        self._analysis_payload = {}
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._ia_validacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._ia_estimacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._limpiar_resultados_ia()
        self._limpiar_resultados_estimacion_ia()
        self._render_cards(self._current_cards)

    def _deseleccionar_todas(self) -> None:
        if not self._current_muestra_id:
            return
        self._selected_fotos_by_muestra[self._current_muestra_id] = set()
        self._analysis_payload = {}
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._ia_validacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._ia_estimacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._limpiar_resultados_ia()
        self._limpiar_resultados_estimacion_ia()
        self._render_cards(self._current_cards)

    def _invertir_seleccion(self) -> None:
        if not self._current_muestra_id:
            return
        fotos = self._fotos_by_muestra.get(self._current_muestra_id, [])
        todos = {str(foto.get("id_foto", "")) for foto in fotos if foto.get("id_foto")}
        actuales = self._selected_fotos_by_muestra.get(self._current_muestra_id, set())
        self._selected_fotos_by_muestra[self._current_muestra_id] = todos.difference(actuales)
        self._analysis_payload = {}
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._ia_validacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._ia_estimacion_resultados_by_muestra[self._current_muestra_id] = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._limpiar_resultados_ia()
        self._limpiar_resultados_estimacion_ia()
        self._render_cards(self._current_cards)

    def _abrir_vista_ampliada(self, card: dict[str, Any]) -> None:
        raw = card.get("raw")
        if not raw or Image is None or ImageTk is None:
            messagebox.showinfo("Obtención calibres", "No hay imagen disponible para ampliar.", parent=self)
            return

        try:
            with Image.open(io.BytesIO(raw)) as img:
                if ImageOps is not None:
                    img = ImageOps.exif_transpose(img)
                ancho, alto = img.size
                max_w, max_h = 1100, 800
                escala = min(max_w / max(ancho, 1), max_h / max(alto, 1), 1.0)
                nuevo_size = (max(int(ancho * escala), 1), max(int(alto * escala), 1))
                if nuevo_size != img.size:
                    img = img.resize(nuevo_size)
                photo = ImageTk.PhotoImage(img.copy())
        except Exception:
            messagebox.showerror("Obtención calibres", "No fue posible abrir la vista ampliada.", parent=self)
            return

        win = tk.Toplevel(self)
        win.title(f"Vista ampliada - {card.get('foto', {}).get('id_foto', '')}")
        cont = ttk.Frame(win, padding=8)
        cont.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        ttk.Label(cont, image=photo).grid(row=0, column=0, sticky="nsew")
        ttk.Label(cont, text=str(card.get("url", "")), foreground="#1b4f72", wraplength=1000).grid(row=1, column=0, sticky="w", pady=(6, 0))
        self._fullsize_refs.append(photo)


def abrir_obtencion_calibres(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Obtención calibres y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, ObtencionCalibresWindow):
            window.lift()
            window.focus_set()
            return

    ObtencionCalibresWindow(parent, db)
