"""Herramienta independiente: obtención de imágenes para cálculo de calibres."""
from __future__ import annotations

import io
import threading
from dataclasses import dataclass
from typing import Any

import requests
import tkinter as tk
from firebase_admin import firestore
from tkinter import messagebox, ttk

from ui_utils import BaseToolWindow

try:
    from PIL import Image, ImageOps, ImageTk
except Exception:  # pragma: no cover - fallback cuando PIL no está disponible
    Image = None
    ImageOps = None
    ImageTk = None

COLLECTION_CONFIG = "Configuraciones"
DOCUMENT_CONFIG = "calibres"


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
        self._preview_refs: list[Any] = []
        self._analysis_payload: dict[str, Any] = {}

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
        frame_fotos.rowconfigure(1, weight=1)
        frame_fotos.columnconfigure(0, weight=1)

        ttk.Button(frame_fotos, text="🧮 Preparar análisis", command=self._preparar_analisis).grid(row=0, column=0, sticky="e", pady=(0, 6))

        self.canvas_fotos = tk.Canvas(frame_fotos, highlightthickness=0)
        self.canvas_fotos.grid(row=1, column=0, sticky="nsew")
        self.scroll_fotos = ttk.Scrollbar(frame_fotos, orient="vertical", command=self.canvas_fotos.yview)
        self.scroll_fotos.grid(row=1, column=1, sticky="ns")
        self.canvas_fotos.configure(yscrollcommand=self.scroll_fotos.set)

        self.frame_fotos_content = ttk.Frame(self.canvas_fotos)
        self._fotos_window = self.canvas_fotos.create_window((0, 0), window=self.frame_fotos_content, anchor="nw")

        self.frame_fotos_content.bind("<Configure>", lambda _: self.canvas_fotos.configure(scrollregion=self.canvas_fotos.bbox("all")))
        self.canvas_fotos.bind("<Configure>", self._sync_fotos_width)

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

    def _buscar_boleta(self) -> None:
        boleta = self.boleta_var.get().strip()
        if not boleta:
            messagebox.showinfo("Obtención calibres", "Ingrese una boleta.", parent=self)
            return

        self.estado_var.set(f"Buscando boleta {boleta}...")
        self._clear_tree()
        self._limpiar_fotos()
        self._fotos_by_muestra = {}
        self._analysis_payload = {}

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

    def _render_fotos_muestra(self, id_muestra: str) -> None:
        self._limpiar_fotos()
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
        for idx, card in enumerate(cards):
            fila = idx // 3
            col = idx % 3
            box = ttk.Frame(self.frame_fotos_content, relief="ridge", padding=6)
            box.grid(row=fila, column=col, padx=6, pady=6, sticky="nsew")

            foto = card["foto"]
            ttk.Label(box, text=f"Foto: {foto['id_foto']}", font=("Segoe UI", 9, "bold")).pack(anchor="w")
            ttk.Label(box, text=f"Ruta: {foto['ruta_local']}", wraplength=280).pack(anchor="w", pady=(2, 4))

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
            self._preview_refs.append(thumb)
            ttk.Label(box, text=card["url"], wraplength=280, foreground="#1b4f72").pack(anchor="w", pady=(4, 0))

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

        self._analysis_payload = {
            "id_muestra": id_muestra,
            "boleta": muestra.get("boleta", ""),
            "cultivo": cultivo,
            "diametro_patron_mm": self._config.diametro_patron_mm if self._config else 94.0,
            "rangos": rangos,
            "fotos": self._fotos_by_muestra.get(id_muestra, []),
        }

        messagebox.showinfo(
            "Obtención calibres",
            (
                "Preparación lista para análisis de calibres.\n\n"
                f"Muestra: {id_muestra}\n"
                f"Cultivo: {cultivo or '-'}\n"
                f"Fotos: {len(self._analysis_payload['fotos'])}\n"
                f"Diámetro patrón: {self._analysis_payload['diametro_patron_mm']:.2f} mm\n"
                f"Rangos configurados: {len(rangos)}"
            ),
            parent=self,
        )

    def get_analysis_payload(self) -> dict[str, Any]:
        """Expone el payload armado para la siguiente etapa (cálculo de calibres)."""
        return dict(self._analysis_payload)


def abrir_obtencion_calibres(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Obtención calibres y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, ObtencionCalibresWindow):
            window.lift()
            window.focus_set()
            return

    ObtencionCalibresWindow(parent, db)
