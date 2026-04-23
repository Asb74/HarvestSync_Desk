"""Herramienta independiente: obtención de imágenes para cálculo de calibres."""
from __future__ import annotations

import io
import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
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


class ObtencionCalibresWindow(BaseToolWindow):
    """UI para flujo boleta -> muestras -> fotos de pantalla de calibres."""

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent, db_firestore)
        self.title("Obtención calibres")
        self.geometry("1220x720")
        self.minsize(1060, 620)

        self.config_repo = CalibresConfigRepository(db_firestore)
        self.data_service = CalibresDataService(db_firestore)

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
        self._overlay_dir = Path(tempfile.gettempdir()) / "harvestsync_desk" / "calibres_overlays"
        self._overlay_dir.mkdir(parents=True, exist_ok=True)
        self._fruit_analyzer = FruitCaliberAnalyzer()
        self._ai_validacion_en_curso = False

        self._build_ui()
        self._cargar_configuracion()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)
        container.columnconfigure(1, weight=2)

        filtros = ttk.LabelFrame(container, text="1) Búsqueda por boleta", padding=10)
        filtros.grid(row=0, column=0, columnspan=2, sticky="ew")
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

        ttk.Label(container, textvariable=self.estado_var, foreground="#34495e").grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 8))

        frame_muestras = ttk.LabelFrame(container, text="2) Muestras asociadas", padding=8)
        frame_muestras.grid(row=2, column=0, sticky="nsew", padx=(0, 8))
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

        frame_fotos = ttk.LabelFrame(container, text="3) Fotos de 'Datos Calibres'", padding=8)
        frame_fotos.grid(row=2, column=1, sticky="nsew")
        frame_fotos.rowconfigure(3, weight=1)
        frame_fotos.columnconfigure(0, weight=1)

        toolbar = ttk.Frame(frame_fotos)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar.columnconfigure(3, weight=1)
        ttk.Button(toolbar, text="Seleccionar todas", command=self._seleccionar_todas).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(toolbar, text="Deseleccionar todas", command=self._deseleccionar_todas).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(toolbar, text="Invertir selección", command=self._invertir_seleccion).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(toolbar, text="🎯 Detectar patrón", command=self._detectar_patron_y_escala).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(toolbar, text="🍊 Analizar frutos", command=self._analizar_frutos).grid(row=0, column=4, padx=(0, 6))
        self.btn_validacion_ia = ttk.Button(toolbar, text="🤖 Validación IA", command=self._ejecutar_validacion_ia)
        self.btn_validacion_ia.grid(row=0, column=5, padx=(0, 6))
        ttk.Button(toolbar, text="🧮 Preparar análisis", command=self._preparar_analisis).grid(row=0, column=6, sticky="e")

        self.resumen_fotos_var = tk.StringVar(value="Fotos encontradas: 0 | Seleccionadas: 0 | Excluidas: 0")
        ttk.Label(frame_fotos, textvariable=self.resumen_fotos_var, foreground="#34495e").grid(row=1, column=0, sticky="w", pady=(0, 6))

        resultados = ttk.LabelFrame(frame_fotos, text="4) Detección patrón y escala", padding=6)
        resultados.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        resultados.columnconfigure(0, weight=1)

        self.tree_resultados = ttk.Treeview(
            resultados,
            columns=("id_foto", "detectado", "diametro_px", "mm_px", "valida", "estado"),
            show="headings",
            height=5,
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
        self.tree_resultados.grid(row=0, column=0, sticky="ew")
        self.tree_resultados.bind("<Double-1>", self._on_double_click_resultado)
        ttk.Button(resultados, text="👁 Ver validación visual", command=self._abrir_overlay_resultado_actual).grid(
            row=1,
            column=0,
            sticky="e",
            pady=(6, 0),
        )

        self.canvas_fotos = tk.Canvas(frame_fotos, highlightthickness=0)
        self.canvas_fotos.grid(row=3, column=0, sticky="nsew")
        self.scroll_fotos = ttk.Scrollbar(frame_fotos, orient="vertical", command=self.canvas_fotos.yview)
        self.scroll_fotos.grid(row=3, column=1, sticky="ns")
        self.canvas_fotos.configure(yscrollcommand=self.scroll_fotos.set)

        self.frame_fotos_content = ttk.Frame(self.canvas_fotos)
        self._fotos_window = self.canvas_fotos.create_window((0, 0), window=self.frame_fotos_content, anchor="nw")

        self.frame_fotos_content.bind("<Configure>", lambda _: self.canvas_fotos.configure(scrollregion=self.canvas_fotos.bbox("all")))
        self.canvas_fotos.bind("<Configure>", self._sync_fotos_width)

        frutos = ttk.LabelFrame(frame_fotos, text="5) Estimación prudente de frutos por calibre", padding=6)
        frutos.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        frutos.columnconfigure(0, weight=1)

        self.tree_frutos_foto = ttk.Treeview(
            frutos,
            columns=("id_foto", "detectados", "validos", "descartados", "descarte_pct", "estado"),
            show="headings",
            height=4,
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
        self.tree_frutos_foto.grid(row=0, column=0, sticky="ew")
        self.tree_frutos_foto.bind("<Double-1>", self._abrir_overlay_frutos_actual)
        ttk.Button(frutos, text="👁 Ver overlay frutos", command=self._abrir_overlay_frutos_actual).grid(row=1, column=0, sticky="e", pady=(6, 0))

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
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()

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
                self.after(0, lambda: self._on_busqueda_error(exc))

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
        self._deteccion_resultados = {}
        self._overlay_paths_by_foto = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
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

    def _preparar_analisis(self) -> None:
        selected = self.tree_muestras.selection()
        if not selected:
            messagebox.showinfo("Obtención calibres", "Seleccione una muestra para preparar análisis.", parent=self)
            return

        id_muestra = selected[0]
        muestra = next((item for item in self._muestras if item["id_muestra"] == id_muestra), None)
        if not muestra:
            messagebox.showerror("Obtención calibres", "No se encontró la muestra seleccionada.", parent=self)
            return

        cultivo = str(muestra.get("cultivo", "")).strip()
        rangos = []
        if self._config:
            rangos = self._config.rangos_por_cultivo.get(cultivo, [])
        fotos_muestra = self._fotos_by_muestra.get(id_muestra, [])
        ids_seleccionadas = self._selected_fotos_by_muestra.get(id_muestra, set())
        fotos_seleccionadas = [foto for foto in fotos_muestra if str(foto.get("id_foto", "")) in ids_seleccionadas]

        if not fotos_seleccionadas:
            messagebox.showwarning(
                "Obtención calibres",
                "No hay fotos seleccionadas para el análisis. Seleccione al menos una foto.",
                parent=self,
            )
            return

        if not self._deteccion_resultados:
            self._detectar_patron_y_escala()

        resultados = {k: v for k, v in self._deteccion_resultados.items() if k in ids_seleccionadas}
        fotos_validas = [
            foto
            for foto in fotos_seleccionadas
            if resultados.get(str(foto.get("id_foto", ""))) and resultados[str(foto.get("id_foto", ""))].valid_for_next_step
        ]

        if not fotos_validas:
            messagebox.showwarning(
                "Obtención calibres",
                "Ninguna foto seleccionada pasó la detección del patrón. Ajuste selección o condiciones de captura.",
                parent=self,
            )
            return

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

    def get_analysis_payload(self) -> dict[str, Any]:
        """Expone el payload armado para la siguiente etapa (cálculo de calibres)."""
        return dict(self._analysis_payload)

    def _ejecutar_validacion_ia(self) -> None:
        if self._ai_validacion_en_curso:
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

        image_path_for_ai, mapping_warning = self._resolve_image_path_for_ai(ruta_local)
        if mapping_warning:
            LOGGER.warning("Validación IA: %s", mapping_warning)

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
            "Validación IA: preparado request. service_url=%s source=%s id_foto=%s ruta_local=%s image_path_enviado=%s",
            service_url,
            source,
            id_foto,
            ruta_local,
            image_path_for_ai,
        )

        self._ai_validacion_en_curso = True
        self.btn_validacion_ia.config(state="disabled")
        self.estado_var.set(f"Validación IA en curso para foto {id_foto}...")
        timeout_seconds = 25

        def worker() -> None:
            t0 = time.perf_counter()
            LOGGER.info(
                "Validación IA: inicio llamada. id_foto=%s endpoint=%s/analyze-image timeout=%ss image_path=%s",
                id_foto,
                service_url.rstrip("/"),
                timeout_seconds,
                image_path_for_ai,
            )
            try:
                result = call_analyze_image(
                    server_url=service_url,
                    image_path=image_path_for_ai,
                    task="validacion_foto",
                    context=(
                        "Evaluar utilidad de imagen para calibres: "
                        "visibilidad general, oclusión, nitidez y presencia/claridad del patrón."
                    ),
                    timeout_seconds=timeout_seconds,
                )
                elapsed = time.perf_counter() - t0
                LOGGER.info("Validación IA: fin correcto. id_foto=%s duracion=%.2fs", id_foto, elapsed)
                self.after(0, lambda: self._on_validacion_ia_ok(id_foto=id_foto, image_path=image_path_for_ai, result=result))
            except InternalAIClientError as exc:
                elapsed = time.perf_counter() - t0
                LOGGER.error("Validación IA: error del servicio interno. id_foto=%s duracion=%.2fs error=%s", id_foto, elapsed, exc)
                self.after(0, lambda: self._on_validacion_ia_error(str(exc)))
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Validación IA: error inesperado en llamada al servicio.")
                self.after(0, lambda: self._on_validacion_ia_error(f"Error inesperado: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def _on_validacion_ia_ok(self, id_foto: str, image_path: str, result: dict[str, Any]) -> None:
        self._ai_validacion_en_curso = False
        self.btn_validacion_ia.config(state="normal")
        self.estado_var.set(f"Validación IA completada para {id_foto}.")
        self._mostrar_resultado_ia(id_foto=id_foto, image_path=image_path, result=result)

    def _on_validacion_ia_error(self, error_message: str) -> None:
        self._ai_validacion_en_curso = False
        self.btn_validacion_ia.config(state="normal")
        self.estado_var.set("Validación IA con error.")
        messagebox.showerror("Obtención calibres - Validación IA", error_message, parent=self)

    def _resolve_image_path_for_ai(self, ruta_local: str) -> tuple[str, str | None]:
        """Devuelve image_path a enviar al servicio IA con diagnóstico de mapeo."""
        candidate = Path(ruta_local)
        if candidate.is_absolute():
            return str(candidate), None

        image_root = os.getenv("HARVESTSYNC_AI_IMAGE_ROOT", "").strip()
        if image_root:
            mapped = Path(image_root) / ruta_local.lstrip("/\\")
            return str(mapped), (
                "ruta_local no es absoluta; se mapea con HARVESTSYNC_AI_IMAGE_ROOT "
                f"({image_root}) -> {mapped}"
            )

        return ruta_local, (
            "ruta_local no es absoluta y no hay HARVESTSYNC_AI_IMAGE_ROOT. "
            "El servidor IA podría no poder acceder al fichero."
        )

    def _mostrar_resultado_ia(self, id_foto: str, image_path: str, result: dict[str, Any]) -> None:
        win = tk.Toplevel(self)
        win.title(f"Resultado Validación IA - {id_foto}")
        win.geometry("760x520")
        win.minsize(600, 420)

        cont = ttk.Frame(win, padding=10)
        cont.grid(row=0, column=0, sticky="nsew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        cont.rowconfigure(1, weight=1)
        cont.columnconfigure(0, weight=1)

        ttk.Label(cont, text=f"Foto: {id_foto}", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(cont, text=f"Ruta enviada: {image_path}", foreground="#1b4f72", wraplength=720).grid(row=0, column=1, sticky="w")

        text = tk.Text(cont, wrap="word")
        text.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        scroll = ttk.Scrollbar(cont, orient="vertical", command=text.yview)
        scroll.grid(row=1, column=2, sticky="ns", pady=(8, 0))
        text.configure(yscrollcommand=scroll.set)

        pretty = json.dumps(result, ensure_ascii=False, indent=2)
        text.insert("1.0", pretty)
        text.configure(state="disabled")

    def _on_toggle_foto(self, id_foto: str, usar_en_analisis: bool) -> None:
        if not self._current_muestra_id or not id_foto:
            return
        seleccionadas = self._selected_fotos_by_muestra.setdefault(self._current_muestra_id, set())
        if usar_en_analisis:
            seleccionadas.add(id_foto)
        else:
            seleccionadas.discard(id_foto)
        self._deteccion_resultados.pop(id_foto, None)
        self._frutos_resultados.pop(id_foto, None)
        self._frutos_overlay_paths_by_foto.pop(id_foto, None)
        self._pintar_resultados_frutos()
        self._actualizar_resumen_fotos()

    def _actualizar_resumen_fotos(self) -> None:
        if not self._current_muestra_id:
            self.resumen_fotos_var.set("Fotos encontradas: 0 | Seleccionadas: 0 | Excluidas: 0")
            return
        total = len(self._fotos_by_muestra.get(self._current_muestra_id, []))
        seleccionadas = len(self._selected_fotos_by_muestra.get(self._current_muestra_id, set()))
        excluidas = max(total - seleccionadas, 0)
        self.resumen_fotos_var.set(
            f"Fotos encontradas: {total} | Seleccionadas: {seleccionadas} | Excluidas: {excluidas}"
        )

    def _seleccionar_todas(self) -> None:
        if not self._current_muestra_id:
            return
        fotos = self._fotos_by_muestra.get(self._current_muestra_id, [])
        self._selected_fotos_by_muestra[self._current_muestra_id] = {
            str(foto.get("id_foto", "")) for foto in fotos if foto.get("id_foto")
        }
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._render_cards(self._current_cards)

    def _deseleccionar_todas(self) -> None:
        if not self._current_muestra_id:
            return
        self._selected_fotos_by_muestra[self._current_muestra_id] = set()
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
        self._render_cards(self._current_cards)

    def _invertir_seleccion(self) -> None:
        if not self._current_muestra_id:
            return
        fotos = self._fotos_by_muestra.get(self._current_muestra_id, [])
        todos = {str(foto.get("id_foto", "")) for foto in fotos if foto.get("id_foto")}
        actuales = self._selected_fotos_by_muestra.get(self._current_muestra_id, set())
        self._selected_fotos_by_muestra[self._current_muestra_id] = todos.difference(actuales)
        self._deteccion_resultados = {}
        self._frutos_resultados = {}
        self._frutos_overlay_paths_by_foto = {}
        self._limpiar_resultados_deteccion()
        self._limpiar_resultados_frutos()
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
