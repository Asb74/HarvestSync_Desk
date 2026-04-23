"""Herramienta independiente: Configuración de calibres por cultivo."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

import tkinter as tk
from firebase_admin import firestore
from tkinter import messagebox, ttk

from ui_utils import BaseToolWindow

COLLECTION_CONFIG = "Configuraciones"
DOCUMENT_CONFIG = "calibres"


class ConfiguracionCalibresWindow(BaseToolWindow):
    """Ventana para parametrizar calibres por cultivo."""

    TREE_COLUMNS = ("cultivo", "nombre_calibre", "desde_mm", "hasta_mm", "orden")

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent, db_firestore)
        self.title("Configuración calibres")
        self.geometry("940x620")
        self.minsize(860, 520)

        self.diametro_patron_var = tk.StringVar(value="94.0")
        self.pantalla_fotos_var = tk.StringVar(value="Datos Calibres")

        self.cultivo_var = tk.StringVar()
        self.nombre_calibre_var = tk.StringVar()
        self.desde_mm_var = tk.StringVar()
        self.hasta_mm_var = tk.StringVar()
        self.orden_var = tk.StringVar()

        self._build_ui()
        self._cargar_configuracion()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=14)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        container.rowconfigure(2, weight=1)
        container.columnconfigure(0, weight=1)

        general_frame = ttk.LabelFrame(container, text="Configuración general", padding=10)
        general_frame.grid(row=0, column=0, sticky="ew")
        general_frame.columnconfigure(1, weight=1)

        ttk.Label(general_frame, text="Diámetro patrón (mm):").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(general_frame, textvariable=self.diametro_patron_var).grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(general_frame, text="Pantalla de fotos:").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(general_frame, textvariable=self.pantalla_fotos_var).grid(row=1, column=1, sticky="ew", pady=4)

        detalle_frame = ttk.LabelFrame(container, text="Rangos por cultivo", padding=10)
        detalle_frame.grid(row=1, column=0, sticky="ew", pady=(12, 8))
        for col in range(10):
            detalle_frame.columnconfigure(col, weight=1)

        ttk.Label(detalle_frame, text="Cultivo").grid(row=0, column=0, sticky="w")
        ttk.Label(detalle_frame, text="Nombre calibre").grid(row=0, column=1, sticky="w")
        ttk.Label(detalle_frame, text="Desde (mm)").grid(row=0, column=2, sticky="w")
        ttk.Label(detalle_frame, text="Hasta (mm)").grid(row=0, column=3, sticky="w")
        ttk.Label(detalle_frame, text="Orden").grid(row=0, column=4, sticky="w")

        ttk.Entry(detalle_frame, textvariable=self.cultivo_var).grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(4, 0))
        ttk.Entry(detalle_frame, textvariable=self.nombre_calibre_var).grid(row=1, column=1, sticky="ew", padx=6, pady=(4, 0))
        ttk.Entry(detalle_frame, textvariable=self.desde_mm_var).grid(row=1, column=2, sticky="ew", padx=6, pady=(4, 0))
        ttk.Entry(detalle_frame, textvariable=self.hasta_mm_var).grid(row=1, column=3, sticky="ew", padx=6, pady=(4, 0))
        ttk.Entry(detalle_frame, textvariable=self.orden_var).grid(row=1, column=4, sticky="ew", padx=6, pady=(4, 0))

        ttk.Button(detalle_frame, text="➕ Añadir fila", command=self._agregar_fila).grid(row=1, column=5, padx=(8, 4), pady=(4, 0), sticky="ew")
        ttk.Button(detalle_frame, text="🗑 Eliminar fila", command=self._eliminar_fila).grid(row=1, column=6, padx=4, pady=(4, 0), sticky="ew")
        ttk.Button(detalle_frame, text="↺ Cargar configuración", command=self._cargar_configuracion).grid(row=1, column=7, padx=4, pady=(4, 0), sticky="ew")
        ttk.Button(detalle_frame, text="💾 Guardar configuración", command=self._guardar_configuracion).grid(row=1, column=8, padx=(4, 0), pady=(4, 0), sticky="ew")

        table_frame = ttk.Frame(container)
        table_frame.grid(row=2, column=0, sticky="nsew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(table_frame, columns=self.TREE_COLUMNS, show="headings", height=14)
        headers = {
            "cultivo": "Cultivo",
            "nombre_calibre": "Nombre calibre",
            "desde_mm": "Desde (mm)",
            "hasta_mm": "Hasta (mm)",
            "orden": "Orden",
        }
        widths = {
            "cultivo": 180,
            "nombre_calibre": 220,
            "desde_mm": 130,
            "hasta_mm": 130,
            "orden": 90,
        }
        for col in self.TREE_COLUMNS:
            anchor = "e" if col in {"desde_mm", "hasta_mm", "orden"} else "w"
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor=anchor)

        scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll_y.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

    def _on_tree_select(self, _: tk.Event) -> None:
        selection = self.tree.selection()
        if not selection:
            return

        values = self.tree.item(selection[0], "values")
        self.cultivo_var.set(values[0])
        self.nombre_calibre_var.set(values[1])
        self.desde_mm_var.set(values[2])
        self.hasta_mm_var.set(values[3])
        self.orden_var.set(values[4])

    def _parse_float(self, value: str, field_name: str) -> float:
        try:
            return float(str(value).strip().replace(",", "."))
        except (ValueError, TypeError):
            raise ValueError(f"El campo '{field_name}' debe ser numérico.") from None

    def _parse_int(self, value: str, field_name: str) -> int:
        text = str(value).strip()
        if not text:
            raise ValueError(f"El campo '{field_name}' es obligatorio.")
        try:
            return int(float(text.replace(",", ".")))
        except (ValueError, TypeError):
            raise ValueError(f"El campo '{field_name}' debe ser entero.") from None

    def _agregar_fila(self) -> None:
        cultivo = self.cultivo_var.get().strip()
        nombre_calibre = self.nombre_calibre_var.get().strip()

        if not cultivo:
            messagebox.showerror("Validación", "El campo 'cultivo' es obligatorio.", parent=self)
            return
        if not nombre_calibre:
            messagebox.showerror("Validación", "El campo 'nombre calibre' es obligatorio.", parent=self)
            return

        try:
            desde_mm = self._parse_float(self.desde_mm_var.get(), "desde_mm")
            hasta_mm = self._parse_float(self.hasta_mm_var.get(), "hasta_mm")
            orden = self._parse_int(self.orden_var.get(), "orden")
        except ValueError as exc:
            messagebox.showerror("Validación", str(exc), parent=self)
            return

        if desde_mm > hasta_mm:
            messagebox.showerror("Validación", "'desde_mm' debe ser menor o igual que 'hasta_mm'.", parent=self)
            return

        self.tree.insert(
            "",
            "end",
            values=(cultivo, nombre_calibre, f"{desde_mm:.2f}", f"{hasta_mm:.2f}", str(orden)),
        )
        self._limpiar_fila_inputs()

    def _eliminar_fila(self) -> None:
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("Configuración calibres", "Selecciona una fila para eliminar.", parent=self)
            return

        for item_id in selected:
            self.tree.delete(item_id)

    def _limpiar_fila_inputs(self) -> None:
        self.cultivo_var.set("")
        self.nombre_calibre_var.set("")
        self.desde_mm_var.set("")
        self.hasta_mm_var.set("")
        self.orden_var.set("")

    def _collect_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item_id in self.tree.get_children(""):
            values = self.tree.item(item_id, "values")
            rows.append(
                {
                    "cultivo": str(values[0]).strip(),
                    "nombre_calibre": str(values[1]).strip(),
                    "desde_mm": self._parse_float(str(values[2]), "desde_mm"),
                    "hasta_mm": self._parse_float(str(values[3]), "hasta_mm"),
                    "orden": self._parse_int(str(values[4]), "orden"),
                }
            )
        return rows

    def _validar_configuracion(self, diametro_patron: float, rows: list[dict[str, Any]]) -> None:
        if diametro_patron <= 0:
            raise ValueError("El diámetro patrón debe ser mayor que 0.")

        grouped_ranges: dict[str, list[tuple[float, float, str]]] = defaultdict(list)
        for idx, row in enumerate(rows, start=1):
            cultivo = row["cultivo"].strip()
            if not cultivo:
                raise ValueError(f"La fila {idx} no tiene cultivo.")

            desde_mm = float(row["desde_mm"])
            hasta_mm = float(row["hasta_mm"])
            if desde_mm > hasta_mm:
                raise ValueError(f"Fila {idx}: 'desde_mm' debe ser menor o igual que 'hasta_mm'.")

            grouped_ranges[cultivo].append((desde_mm, hasta_mm, row["nombre_calibre"].strip()))

        for cultivo, ranges in grouped_ranges.items():
            ordered = sorted(ranges, key=lambda value: (value[0], value[1]))
            for i in range(1, len(ordered)):
                previous = ordered[i - 1]
                current = ordered[i]
                if current[0] <= previous[1]:
                    raise ValueError(
                        "Hay rangos solapados en cultivo "
                        f"'{cultivo}' entre '{previous[2]}' ({previous[0]}-{previous[1]}) "
                        f"y '{current[2]}' ({current[0]}-{current[1]})."
                    )

    def _cargar_configuracion(self) -> None:
        try:
            doc = self.db.collection(COLLECTION_CONFIG).document(DOCUMENT_CONFIG).get()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Configuración calibres", f"No se pudo cargar desde Firestore: {exc}", parent=self)
            return

        if not doc.exists:
            self._cargar_defaults()
            return

        data = doc.to_dict() or {}
        diametro = data.get("diametro_patron_mm", 94.0)
        pantalla = data.get("pantalla_fotos", "Datos Calibres")
        rangos = data.get("rangos", [])

        self.diametro_patron_var.set(str(diametro))
        self.pantalla_fotos_var.set(str(pantalla or "Datos Calibres"))

        for item_id in self.tree.get_children(""):
            self.tree.delete(item_id)

        for row in rangos:
            cultivo = str(row.get("cultivo", "")).strip()
            nombre_calibre = str(row.get("nombre_calibre", "")).strip()
            desde_mm = float(row.get("desde_mm", 0.0))
            hasta_mm = float(row.get("hasta_mm", 0.0))
            orden = int(row.get("orden", 0))
            self.tree.insert(
                "",
                "end",
                values=(cultivo, nombre_calibre, f"{desde_mm:.2f}", f"{hasta_mm:.2f}", str(orden)),
            )

    def _cargar_defaults(self) -> None:
        self.diametro_patron_var.set("94.0")
        self.pantalla_fotos_var.set("Datos Calibres")
        for item_id in self.tree.get_children(""):
            self.tree.delete(item_id)

    def _guardar_configuracion(self) -> None:
        try:
            diametro_patron = self._parse_float(self.diametro_patron_var.get(), "diametro_patron_mm")
            pantalla_fotos = self.pantalla_fotos_var.get().strip() or "Datos Calibres"
            rows = self._collect_rows()
            rows = sorted(rows, key=lambda item: (item["cultivo"].lower(), item["orden"], item["desde_mm"]))
            self._validar_configuracion(diametro_patron, rows)
        except ValueError as exc:
            messagebox.showerror("Validación", str(exc), parent=self)
            return

        payload = {
            "diametro_patron_mm": diametro_patron,
            "pantalla_fotos": pantalla_fotos,
            "rangos": rows,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        try:
            self.db.collection(COLLECTION_CONFIG).document(DOCUMENT_CONFIG).set(payload, merge=True)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Configuración calibres", f"No se pudo guardar en Firestore: {exc}", parent=self)
            return

        messagebox.showinfo("Configuración calibres", "Configuración guardada correctamente.", parent=self)


def abrir_configuracion_calibres(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Configuración calibres y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, ConfiguracionCalibresWindow):
            window.lift()
            window.focus_set()
            return

    ConfiguracionCalibresWindow(parent, db)
