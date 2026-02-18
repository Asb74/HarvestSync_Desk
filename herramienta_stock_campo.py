"""Herramienta independiente: Stock de Campo."""
from __future__ import annotations

import datetime
import os
import subprocess
import sys
import tempfile
import threading
import traceback
from pathlib import Path
from typing import Any

import sqlite3
import tkinter as tk
from firebase_admin import firestore
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from tkinter import messagebox, ttk

from ui_utils import BaseToolWindow

DB_FRUTA_PATH = r"X:\\BasesSQLite\\DBfruta.sqlite"
DB_CALIDAD_PATH = r"X:\\BasesSQLite\\Partidas.BdCalidad.sqlite"
ACTUALIZACIONES_PATH = Path(r"X:\\BasesSQLite\\ultima_actualizacion.txt")


class StockCampoWindow(BaseToolWindow):
    """Ventana para calcular y exportar el Stock de Campo pendiente de volcar."""

    COLOR_MAP = {
        "ROJO": "#ffd6d6",
        "AMARILLO": "#fff7bf",
        "VERDE": "#d8f5d0",
        "GRIS": "#d9d9d9",
    }
    FILTER_CONFIG = {
        "Cultivo": "Cultivo",
        "Empresa": "Empresa",
        "F. Carga": "Fcarga",
        "Socio": "Socio",
        "Variedad": "Variedad",
        "Color": "Color",
        "Boleta": "Boleta",
        "Plataforma": "Plataforma",
    }
    SEGMENTADOR_SECTIONS = ("Cultivo", "Empresa")

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent, db_firestore)
        self.title("Stock de Campo")
        self.geometry("1200x650")

        self.total_general_var = tk.StringVar(value="TOTAL GENERAL: 0 kg")
        self.actualizacion_var = tk.StringVar(value="Última actualización: No disponible")

        self._rows: list[dict[str, Any]] = []
        self._raw_rows: list[dict[str, Any]] = []
        self._temp_logo_path: str | None = None
        self._syncing_filters = False
        self._selected_values = {section: set() for section in self.FILTER_CONFIG}
        self._sort_column = "#0"
        self._sort_descending = False

        self._build_ui()
        self._actualizar_label_actualizacion()
        self._lanzar_calculo(disable_buttons=False)

    def _build_ui(self) -> None:
        style = ttk.Style()
        bg = self.cget("background") or style.lookup("TFrame", "background")

        container = ttk.Frame(self, padding=10)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        frame_filtros = ttk.LabelFrame(container, text="Filtros", padding=10)
        frame_filtros.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        frame_filtros.columnconfigure(0, weight=1)
        frame_filtros.columnconfigure(1, weight=0)

        self._build_segmentadores(frame_filtros)

        frame_acciones = ttk.Frame(frame_filtros)
        frame_acciones.grid(row=0, column=1, sticky="e")

        self.btn_calcular = ttk.Button(frame_acciones, text="🔍 Calcular", command=self._lanzar_calculo)
        self.btn_calcular.grid(row=0, column=0, padx=6, pady=(0, 6), sticky="e")

        self.btn_reset_filtros = ttk.Button(frame_acciones, text="♻ Reiniciar filtros", command=self._reiniciar_filtros)
        self.btn_reset_filtros.grid(row=0, column=1, padx=6, pady=(0, 6), sticky="e")

        self.btn_exportar = ttk.Button(frame_acciones, text="👁 Ver PDF", command=self._exportar_pdf)
        self.btn_exportar.grid(row=0, column=2, padx=6, pady=(0, 6), sticky="e")

        frame_dashboard = ttk.Frame(container)
        frame_dashboard.grid(row=1, column=0, sticky="nsew")
        frame_dashboard.rowconfigure(0, weight=1)
        frame_dashboard.rowconfigure(1, weight=0)
        frame_dashboard.columnconfigure(0, weight=7)
        frame_dashboard.columnconfigure(1, weight=3)

        frame_left = ttk.Frame(frame_dashboard, padding=(0, 0, 8, 0))
        frame_left.grid(row=0, column=0, sticky="nsew")
        frame_left.rowconfigure(1, weight=1)
        frame_left.columnconfigure(0, weight=1)

        self.lbl_actualizacion = ttk.Label(
            frame_left,
            textvariable=self.actualizacion_var,
            font=("Segoe UI", 10),
            anchor="e",
        )
        self.lbl_actualizacion.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        frame_tabla = ttk.Frame(frame_left)
        frame_tabla.grid(row=1, column=0, sticky="nsew")
        frame_tabla.rowconfigure(0, weight=1)
        frame_tabla.columnconfigure(0, weight=1)

        columns = ("Boleta", "Plataforma", "Empresa", "Cultivo", "Variedad", "Restricciones", "KilosPendientes")
        self.tree = ttk.Treeview(frame_tabla, columns=columns, show="headings", height=18)
        for col in columns:
            anchor = "e" if col == "KilosPendientes" else "w"
            width = 170 if col != "KilosPendientes" else 150
            heading_text = "Neto" if col == "KilosPendientes" else col
            self.tree.heading(col, text=heading_text, command=lambda current_col=col: self._on_tree_heading_click(current_col))
            self.tree.column(col, width=width, anchor=anchor)

        self._configured_color_tags: set[str] = set()
        for color_name, color_hex in self.COLOR_MAP.items():
            tag_name = f"COLOR_{color_name.upper()}"
            self.tree.tag_configure(tag_name, background=color_hex)
            self._configured_color_tags.add(tag_name)

        scroll_y = ttk.Scrollbar(frame_tabla, orient="vertical", command=self.tree.yview)
        scroll_x = ttk.Scrollbar(frame_tabla, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        frame_right = ttk.Frame(frame_dashboard, padding=(8, 0, 0, 0))
        frame_right.grid(row=0, column=1, sticky="nsew")
        frame_right.rowconfigure(0, weight=1)
        frame_right.columnconfigure(0, weight=1)

        panel_scroll = ttk.Scrollbar(frame_right, orient="vertical")
        panel_scroll.grid(row=0, column=1, sticky="ns")

        panel_canvas = tk.Canvas(frame_right, highlightthickness=0, background=bg)
        panel_canvas.grid(row=0, column=0, sticky="nsew")
        panel_canvas.configure(yscrollcommand=panel_scroll.set)
        panel_scroll.configure(command=panel_canvas.yview)
        self._enable_mousewheel(panel_canvas)

        panel_content = ttk.Frame(panel_canvas)
        panel_window = panel_canvas.create_window((0, 0), window=panel_content, anchor="nw")

        def _sync_panel_width(event: tk.Event) -> None:
            panel_canvas.itemconfigure(panel_window, width=event.width)

        def _sync_panel_region(_: tk.Event) -> None:
            panel_canvas.configure(scrollregion=panel_canvas.bbox("all"))

        panel_canvas.bind("<Configure>", _sync_panel_width)
        panel_content.bind("<Configure>", _sync_panel_region)

        self.side_filter_blocks: dict[str, dict[str, Any]] = {}
        side_sections = ["F. Carga", "Socio", "Variedad", "Color", "Boleta", "Plataforma"]

        for row_idx, section_name in enumerate(side_sections):
            block = ttk.LabelFrame(panel_content, text=section_name, padding=8)
            block.grid(row=row_idx, column=0, sticky="nsew", pady=(0, 10))
            block.rowconfigure(0, weight=1)
            block.columnconfigure(0, weight=1)

            block_canvas = tk.Canvas(block, height=110, highlightthickness=0, background=bg)
            block_canvas.grid(row=0, column=0, sticky="nsew")
            block_scroll = ttk.Scrollbar(block, orient="vertical", command=block_canvas.yview)
            block_scroll.grid(row=0, column=1, sticky="ns")
            block_canvas.configure(yscrollcommand=block_scroll.set)
            self._enable_mousewheel(block_canvas)

            block_content = ttk.Frame(block_canvas)
            block_window = block_canvas.create_window((0, 0), window=block_content, anchor="nw")

            def _sync_block_width(event: tk.Event, canvas: tk.Canvas = block_canvas, win: int = block_window) -> None:
                canvas.itemconfigure(win, width=event.width)

            def _sync_block_region(_: tk.Event, canvas: tk.Canvas = block_canvas) -> None:
                canvas.configure(scrollregion=canvas.bbox("all"))

            block_canvas.bind("<Configure>", _sync_block_width)
            block_content.bind("<Configure>", _sync_block_region)

            self.side_filter_blocks[section_name] = {
                "labelframe": block,
                "canvas": block_canvas,
                "content": block_content,
                "scrollbar": block_scroll,
                "variables": [],
                "checkbuttons": [],
            }

        panel_content.columnconfigure(0, weight=1)

        ttk.Label(frame_dashboard, textvariable=self.total_general_var, font=("Segoe UI", 11, "bold")).grid(
            row=1,
            column=0,
            columnspan=2,
            sticky="e",
            padx=(0, 6),
            pady=(8, 0),
        )

    def _build_segmentadores(self, parent: ttk.LabelFrame) -> None:
        self.frame_segmentadores = ttk.Frame(parent)
        self.frame_segmentadores.grid(row=0, column=0, sticky="ew", padx=(0, 10), pady=(0, 6))
        self.frame_segmentadores.columnconfigure(0, weight=1)
        self.frame_segmentadores.columnconfigure(1, weight=1)

        self.segmentador_blocks: dict[str, dict[str, Any]] = {}
        for idx, section in enumerate(self.SEGMENTADOR_SECTIONS):
            block = ttk.LabelFrame(self.frame_segmentadores, text=section.upper(), padding=8)
            block.grid(row=0, column=idx, sticky="ew", padx=(0, 8) if idx == 0 else (8, 0))
            block.columnconfigure(0, weight=1)

            content = ttk.Frame(block)
            content.grid(row=0, column=0, sticky="ew")
            content.columnconfigure(0, weight=1)

            self.segmentador_blocks[section] = {
                "container": block,
                "content": content,
                "buttons": {},
                "all_values": [],
                "last_max_cols": None,
                "resize_after_id": None,
            }
            content.bind(
                "<Configure>",
                lambda _, current_section=section: self._schedule_segmentador_resize(current_section),
            )

    def _schedule_segmentador_resize(self, section: str) -> None:
        block = self.segmentador_blocks.get(section)
        if not block:
            return

        resize_after_id = block.get("resize_after_id")
        if resize_after_id:
            self.after_cancel(resize_after_id)

        block["resize_after_id"] = self.after(100, lambda: self._rerender_segmentador_for_resize(section))

    def _rerender_segmentador_for_resize(self, section: str) -> None:
        block = self.segmentador_blocks.get(section)
        if not block:
            return

        block["resize_after_id"] = None
        values = list(block.get("all_values", []))
        if not values:
            return

        self._render_segmentador(section, values)
        self._update_segmentador_visual(section)

    def _calculate_segmentador_max_cols(self, section: str) -> int:
        block = self.segmentador_blocks.get(section)
        if not block:
            return 1

        width = block["content"].winfo_width()
        if width <= 1:
            width = 800

        estimated_button_width = 120
        return max(1, width // estimated_button_width)

    def _enable_mousewheel(self, canvas: tk.Canvas) -> None:
        def _on_mousewheel(event: tk.Event, target_canvas: tk.Canvas = canvas) -> None:
            if event.delta:
                target_canvas.yview_scroll(int(-event.delta / 120), "units")

        def _bind_wheel(_: tk.Event, target_canvas: tk.Canvas = canvas) -> None:
            target_canvas.bind("<MouseWheel>", _on_mousewheel)

        def _unbind_wheel(_: tk.Event, target_canvas: tk.Canvas = canvas) -> None:
            target_canvas.unbind("<MouseWheel>")

        canvas.bind("<Enter>", _bind_wheel)
        canvas.bind("<Leave>", _unbind_wheel)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(DB_FRUTA_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute(
            f"ATTACH DATABASE '{DB_CALIDAD_PATH.replace('\\', '/')}' AS bdcalidad;"
        )
        return conn

    def _construir_sql(self) -> tuple[str, list]:
        sql = """
        SELECT
            p.AlbaranDef,
            COALESCE(p.Socio,'') AS Socio,
            p.Fcarga,
            COALESCE(p.Variedad,'') AS Variedad,
            COALESCE(p.Boleta,'') AS Boleta,
            COALESCE(p.Plataforma,'') AS Plataforma,
            COALESCE(p.EMPRESA,'') AS EMPRESA,
            COALESCE(p.CULTIVO,'') AS CULTIVO,
            p.Neto,
            p.NetoPartida,
            COALESCE(p.Restricciones,'') AS Restricciones,
            COALESCE(m.Valor,'') AS Color
        FROM PesosFres p
        LEFT JOIN MRestricciones m
            ON m.IdRestricciones = p.Restricciones
           AND m.CULTIVO = p.CULTIVO
        LEFT JOIN bdcalidad.PartidasIndex pi
            ON p.AlbaranDef = pi.IdPartida
        WHERE
            p.AlbaranDef IS NOT NULL
            AND p.AlbaranDef <> ''
            AND pi.IdPartida IS NULL
    """
        return sql, []

    def _obtener_ultima_actualizacion(self) -> str:
        try:
            if not ACTUALIZACIONES_PATH.exists():
                return "No disponible"

            lineas = ACTUALIZACIONES_PATH.read_text(
                encoding="utf-8",
                errors="ignore"
            ).splitlines()

            for linea in lineas:
                linea = linea.strip()
                if linea and "/" in linea and ":" in linea:
                    return linea

            return "No disponible"

        except Exception:
            return "No disponible"

    def _actualizar_label_actualizacion(self) -> None:
        self.actualizacion_var.set(f"Última actualización: {self._obtener_ultima_actualizacion()}")

    def _lanzar_calculo(self, disable_buttons: bool = True) -> None:
        self._actualizar_label_actualizacion()

        if disable_buttons:
            self.btn_calcular.configure(state="disabled")
            self.btn_exportar.configure(state="disabled")
            self.btn_reset_filtros.configure(state="disabled")

        def worker() -> None:
            try:
                rows = self._ejecutar_consulta()
                self.after(0, lambda: self._mostrar_resultados(rows))
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc()
                self.after(
                    0,
                    lambda: messagebox.showerror(
                        "Stock de Campo",
                        f"Error en el cálculo:\n\nEXCEPCIÓN: {repr(exc)}\n\nTRACEBACK COMPLETO:\n{tb}",
                    ),
                )
            finally:
                if disable_buttons:
                    self.after(0, lambda: self.btn_calcular.configure(state="normal"))
                    self.after(0, lambda: self.btn_exportar.configure(state="normal"))
                    self.after(0, lambda: self.btn_reset_filtros.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    def _ejecutar_consulta(self) -> list[dict[str, Any]]:
        sql, _ = self._construir_sql()
        print("----- SQL GENERADA -----")
        print(sql)
        print("PARAMS: []")
        print("------------------------")
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            data: list[dict[str, Any]] = []
            for row in cur.fetchall():
                neto_partida = 0.0 if row["NetoPartida"] is None else float(row["NetoPartida"])
                neto = 0.0 if row["Neto"] is None else float(row["Neto"])
                kilos = neto_partida if neto_partida != 0 else neto
                fcarga = row["Fcarga"]

                if isinstance(fcarga, str) and fcarga:
                    try:
                        dt = datetime.datetime.fromisoformat(fcarga)
                        fcarga_fmt = dt.strftime("%d/%m/%Y")
                    except Exception:
                        fcarga_fmt = fcarga
                else:
                    fcarga_fmt = ""
                data.append(
                    {
                        "AlbaranDef": "" if row["AlbaranDef"] is None else str(row["AlbaranDef"]),
                        "Socio": "" if row["Socio"] is None else str(row["Socio"]),
                        "Fcarga": fcarga_fmt,
                        "Boleta": "" if row["Boleta"] is None else str(row["Boleta"]),
                        "Plataforma": "" if row["Plataforma"] is None else str(row["Plataforma"]),
                        "Empresa": "" if row["EMPRESA"] is None else str(row["EMPRESA"]),
                        "Cultivo": "" if row["CULTIVO"] is None else str(row["CULTIVO"]),
                        "Variedad": "" if row["Variedad"] is None else str(row["Variedad"]),
                        "Restricciones": "" if row["Restricciones"] is None else str(row["Restricciones"]),
                        "Color": "" if row["Color"] is None else str(row["Color"]),
                        "KilosPendientes": kilos,
                    }
                )
        finally:
            conn.close()
        self._raw_rows = data
        return data

    def _mostrar_resultados(self, rows: list[dict[str, Any]]) -> None:
        if self.tree["show"] != "tree headings":
            self.tree.configure(show="tree headings")
            self.tree.heading("#0", text="Detalle", command=lambda: self._on_tree_heading_click("#0"))
            self.tree.column("#0", width=260, anchor="w")
        self._update_tree_headings_visual()
        self._populate_side_filters(rows)
        self._apply_side_filters()

    def _on_tree_heading_click(self, column: str) -> None:
        if self._sort_column == column:
            self._sort_descending = not self._sort_descending
        else:
            self._sort_column = column
            self._sort_descending = False
        self._update_tree_headings_visual()
        self._render_tree_rows(self._rows)

    def _update_tree_headings_visual(self) -> None:
        base_headings = {
            "#0": "Detalle",
            "Boleta": "Boleta",
            "Plataforma": "Plataforma",
            "Empresa": "Empresa",
            "Cultivo": "Cultivo",
            "Variedad": "Variedad",
            "Restricciones": "Restricciones",
            "KilosPendientes": "Neto",
        }
        for column in ("#0", *self.tree["columns"]):
            heading_text = base_headings.get(column, str(column))
            if column == self._sort_column:
                arrow = "▼" if self._sort_descending else "▲"
                heading_text = f"{heading_text} {arrow}"
            self.tree.heading(column, text=heading_text)

    def _get_item_sort_key(self, item: tuple[str, dict[str, Any], float]) -> Any:
        albaran, row, kilos = item
        albaran_limpio = self._value_or_empty(albaran)
        sort_map = {
            "#0": albaran_limpio,
            "Boleta": self._value_or_empty(row.get("Boleta")),
            "Plataforma": self._value_or_empty(row.get("Plataforma")),
            "Empresa": self._value_or_empty(row.get("Empresa")),
            "Cultivo": self._value_or_empty(row.get("Cultivo")),
            "Variedad": self._value_or_empty(row.get("Variedad")),
            "Restricciones": self._value_or_empty(row.get("Color")).upper(),
            "KilosPendientes": float(kilos),
        }
        value = sort_map.get(self._sort_column, albaran_limpio)
        if isinstance(value, str):
            return value.casefold()
        return value

    def _populate_side_filters(self, rows: list[dict[str, Any]]) -> None:
        for section, field in self.FILTER_CONFIG.items():
            unique_values = sorted({self._value_or_empty(row.get(field)) for row in rows})
            current_selection = set(self._selected_values.get(section, set()))
            self._selected_values[section] = {value for value in current_selection if value in set(unique_values)}
            if section in self.SEGMENTADOR_SECTIONS:
                self._render_segmentador(section, unique_values)
            else:
                self._render_side_filter_block(section, unique_values, self._selected_values[section])

    def _render_segmentador(self, section: str, values: list[str]) -> None:
        block = self.segmentador_blocks.get(section)
        if not block:
            return

        todo_key = "__TODO__"
        values_with_todo = [todo_key, *values]

        content = block["content"]
        content_width = content.winfo_width()
        if content_width <= 1:
            content_width = block["container"].winfo_width()
        if content_width <= 1:
            content_width = 800

        min_button_width_px = 120
        horizontal_gap = 8
        max_cols = max(1, content_width // (min_button_width_px + horizontal_gap))
        max_cols = min(max_cols, len(values_with_todo))

        same_values = set(values) == set(block["all_values"])
        same_layout = block.get("last_max_cols") == max_cols
        if same_values and same_layout:
            self._update_segmentador_visual(section)
            return

        for button in block["buttons"].values():
            button.destroy()

        existing_cols = max(content.grid_size()[0], max_cols)
        for col in range(existing_cols):
            content.columnconfigure(col, weight=0, uniform="")

        for col in range(max_cols):
            content.columnconfigure(col, weight=1, uniform=f"segment_{section}")

        block["buttons"] = {}
        block["all_values"] = list(values)
        block["last_max_cols"] = max_cols

        for idx, value in enumerate(values_with_todo):
            button_text = "(Todo)" if value == todo_key else (value or "(Vacío)")
            button = tk.Button(
                content,
                text=button_text,
                relief="raised",
                bd=1,
                padx=8,
                pady=2,
                command=lambda selected_value=value, selected_section=section: self._toggle_segmentador_value(
                    selected_section,
                    selected_value,
                ),
            )
            row = idx // max_cols
            col = idx % max_cols
            button.grid(row=row, column=col, padx=4, pady=(0, 4), sticky="ew")
            block["buttons"][value] = button

        self._update_segmentador_visual(section)

    def _toggle_segmentador_value(self, section: str, value: str) -> None:
        selected = self._selected_values.setdefault(section, set())
        if value == "__TODO__":
            self._selected_values[section] = set()
        else:
            if value in selected:
                selected.remove(value)
            else:
                selected.add(value)
        self._update_segmentador_visual(section)
        self._apply_side_filters()

    def _update_segmentador_visual(self, section: str) -> None:
        block = self.segmentador_blocks.get(section)
        if not block:
            return
        todo_key = "__TODO__"
        selected_values = self._selected_values.get(section, set())
        for value, button in block["buttons"].items():
            is_selected = (not selected_values) if value == todo_key else value in selected_values
            button.configure(
                relief="sunken" if is_selected else "raised",
                background="#2f2f2f" if is_selected else "#f2f2f2",
                foreground="#ffffff" if is_selected else "#000000",
                activebackground="#3d3d3d" if is_selected else "#e5e5e5",
                activeforeground="#ffffff" if is_selected else "#000000",
            )

    def _render_side_filter_block(self, section: str, values: list[str], selected_values: set[str]) -> None:
        block = self.side_filter_blocks.get(section)
        if not block:
            return
        for check in block.get("checkbuttons", []):
            check.destroy()
        block["variables"] = []
        block["checkbuttons"] = []
        block["all_values"] = list(values)

        select_all_var = tk.BooleanVar(value=bool(values) and set(selected_values) == set(values))

        def on_select_all_change() -> None:
            if self._syncing_filters:
                return
            should_select = select_all_var.get()
            if should_select:
                self._selected_values[section] = set(values)
            else:
                self._selected_values[section] = set()
            self._syncing_filters = True
            for _, item_var in block["variables"]:
                item_var.set(should_select)
            self._syncing_filters = False
            self._apply_side_filters()

        def on_item_change() -> None:
            if self._syncing_filters:
                return
            selected = {v for v, var in block["variables"] if var.get()}
            self._selected_values[section] = selected
            all_marked = bool(block["variables"]) and all(var.get() for _, var in block["variables"])
            self._syncing_filters = True
            select_all_var.set(all_marked)
            self._syncing_filters = False
            self._apply_side_filters()

        select_all_check = ttk.Checkbutton(
            block["content"],
            text="Seleccionar todos",
            variable=select_all_var,
            command=on_select_all_change,
        )
        select_all_check.pack(fill="x", anchor="w")
        block["select_all_var"] = select_all_var
        block["checkbuttons"].append(select_all_check)

        for value in values:
            var = tk.BooleanVar(value=value in selected_values)
            check = ttk.Checkbutton(
                block["content"],
                text=value or "(Vacío)",
                variable=var,
                command=on_item_change,
            )
            check.pack(fill="x", anchor="w")
            block["variables"].append((value, var))
            block["checkbuttons"].append(check)

        self._syncing_filters = True
        select_all_var.set(bool(block["variables"]) and all(var.get() for _, var in block["variables"]))
        self._syncing_filters = False

    def _value_or_empty(self, value: Any) -> str:
        return "" if value is None else str(value).strip()

    def _get_selected_values(self, section: str) -> set[str]:
        return self._selected_values.get(section, set())

    def _filter_rows_by_selection(
        self,
        rows: list[dict[str, Any]],
        selected_by_section: dict[str, set[str]],
        ignored_section: str | None = None,
    ) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for row in rows:
            include = True
            for section, field in self.FILTER_CONFIG.items():
                if section == ignored_section:
                    continue
                allowed = selected_by_section.get(section, set())
                value = self._value_or_empty(row.get(field))
                if allowed and value not in allowed:
                    include = False
                    break
            if include:
                filtered.append(row)
        return filtered

    def _apply_side_filters(self) -> None:
        if self._syncing_filters:
            return

        raw_rows = self._raw_rows

        # 1️⃣ Construir selección activa (si vacío = todos)
        selected_by_section = {}
        for section, field in self.FILTER_CONFIG.items():
            selected = self._selected_values.get(section, set())
            if not selected:
                selected_by_section[section] = {
                    self._value_or_empty(r.get(field)) for r in raw_rows
                }
            else:
                selected_by_section[section] = selected

        # 2️⃣ Aplicar filtros al dataset
        filtered_rows = self._filter_rows_by_selection(raw_rows, selected_by_section)
        self._rows = filtered_rows

        # 3️⃣ Recalcular valores disponibles por bloque dinámicamente
        for section, field in self.FILTER_CONFIG.items():
            rows_for_section = self._filter_rows_by_selection(raw_rows, selected_by_section, ignored_section=section)
            available_values = {self._value_or_empty(r.get(field)) for r in rows_for_section}

            # Eliminar selecciones que ya no existan
            current_selection = self._selected_values.get(section, set())
            valid_selection = current_selection.intersection(available_values)
            self._selected_values[section] = valid_selection

            # Volver a renderizar bloque con selección válida
            if section in self.SEGMENTADOR_SECTIONS:
                self._render_segmentador(section, sorted(available_values))
                self._update_segmentador_visual(section)
            else:
                self._render_side_filter_block(
                    section,
                    sorted(available_values),
                    valid_selection,
                )

        # 4️⃣ Renderizar árbol
        self._render_tree_rows(self._rows)

    def _render_tree_rows(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        estructura, total_general = self._build_hierarchical_structure(rows)
        for cultivo, cultivo_node in estructura.items():
            cultivo_iid = self.tree.insert(
                "",
                "end",
                text=cultivo,
                values=("", "", "", "", "", "", self._format_kilos(cultivo_node["total"])),
                open=True,
            )
            for variedad, variedad_node in cultivo_node["variedades"].items():
                variedad_iid = self.tree.insert(
                    cultivo_iid,
                    "end",
                    text=variedad,
                    values=("", "", "", "", "", "", self._format_kilos(variedad_node["total"])),
                    open=True,
                )
                for socio, socio_node in variedad_node["socios"].items():
                    socio_iid = self.tree.insert(
                        variedad_iid,
                        "end",
                        text=socio,
                        values=("", "", "", "", "", "", self._format_kilos(socio_node["total"])),
                        open=True,
                    )
                    for albaran, row, kilos in sorted(
                        socio_node["items"],
                        key=self._get_item_sort_key,
                        reverse=self._sort_descending,
                    ):
                        color = self._value_or_empty(row.get("Color")).upper()
                        tag_name = ""
                        if color:
                            tag_name = f"COLOR_{color}"
                            if tag_name not in self._configured_color_tags and color in self.COLOR_MAP:
                                self.tree.tag_configure(tag_name, background=self.COLOR_MAP[color])
                                self._configured_color_tags.add(tag_name)
                        restr_display = f"■ {color}" if color else ""
                        boleta = self._value_or_empty(row.get("Boleta"))
                        self.tree.insert(
                            socio_iid,
                            "end",
                            text=albaran,
                            values=(
                                boleta,
                                self._value_or_empty(row.get("Plataforma")),
                                self._value_or_empty(row.get("Empresa")),
                                self._value_or_empty(row.get("Cultivo")),
                                self._value_or_empty(row.get("Variedad")),
                                restr_display,
                                self._format_kilos(kilos),
                            ),
                            tags=(tag_name,) if tag_name else (),
                        )
        total_fmt = self._format_kilos(total_general)
        self.total_general_var.set(f"TOTAL GENERAL: {total_fmt} kg")

    def _build_hierarchical_structure(self, rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], float]:
        estructura: dict[str, dict[str, Any]] = {}
        total_general = 0.0
        for row in rows:
            cultivo = self._value_or_empty(row.get("Cultivo")) or "(Sin cultivo)"
            variedad = self._value_or_empty(row.get("Variedad")) or "(Sin variedad)"
            socio = self._value_or_empty(row.get("Socio")) or "(Sin socio)"
            albaran = self._value_or_empty(row.get("AlbaranDef")) or "(Sin albarán)"
            kilos = float(row.get("KilosPendientes") or 0.0)
            total_general += kilos
            if cultivo not in estructura:
                estructura[cultivo] = {"total": 0.0, "variedades": {}}
            estructura[cultivo]["total"] += kilos
            variedades = estructura[cultivo]["variedades"]
            if variedad not in variedades:
                variedades[variedad] = {"total": 0.0, "socios": {}}
            variedades[variedad]["total"] += kilos
            socios = variedades[variedad]["socios"]
            if socio not in socios:
                socios[socio] = {"total": 0.0, "items": []}
            socios[socio]["total"] += kilos
            socios[socio]["items"].append((albaran, row, kilos))
        return estructura, total_general

    def _reiniciar_filtros(self) -> None:
        self._syncing_filters = True
        for section, field in self.FILTER_CONFIG.items():
            self._selected_values[section] = set()
            full_values = sorted({self._value_or_empty(row.get(field)) for row in self._raw_rows})
            if section in self.SEGMENTADOR_SECTIONS:
                self._render_segmentador(section, full_values)
            else:
                for _, var in self.side_filter_blocks.get(section, {}).get("variables", []):
                    var.set(False)
                self._render_side_filter_block(section, full_values, set())
        self._syncing_filters = False
        self._apply_side_filters()

    def _format_kilos(self, kilos: float) -> str:
        return f"{kilos:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def _resolve_logo_for_pdf(self) -> str | None:
        master_logo = getattr(self.master, "logo_icon", None)
        if master_logo is not None:
            photo = getattr(master_logo, "_PhotoImage__photo", None)
            if photo is not None and hasattr(photo, "write"):
                tmp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
                tmp_file.close()
                try:
                    photo.write(tmp_file.name, format="png")
                    self._temp_logo_path = tmp_file.name
                    return tmp_file.name
                except Exception:  # noqa: BLE001
                    try:
                        os.unlink(tmp_file.name)
                    except OSError:
                        pass

        for candidate in ("icono_app.png", "COOPERATIVA.png"):
            path = Path(__file__).resolve().parent / candidate
            if path.exists():
                return str(path)
        return None

    def _exportar_pdf(self) -> None:
        if not self._rows:
            messagebox.showwarning("Stock de Campo", "No hay resultados para visualizar en PDF.")
            return

        temp_pdf = tempfile.NamedTemporaryFile(
            suffix=f"_stock_campo_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            delete=False,
        )
        temp_pdf.close()

        try:
            self._crear_pdf(temp_pdf.name)
            if hasattr(os, "startfile"):
                os.startfile(temp_pdf.name)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", temp_pdf.name])
            else:
                subprocess.Popen(["xdg-open", temp_pdf.name])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Stock de Campo", f"Error generando PDF:\n{exc}")
        finally:
            if self._temp_logo_path and os.path.exists(self._temp_logo_path):
                try:
                    os.unlink(self._temp_logo_path)
                except OSError:
                    pass
                self._temp_logo_path = None

    def _crear_pdf(self, filename: str) -> None:
        doc = SimpleDocTemplate(
            filename,
            pagesize=landscape(A4),
            leftMargin=1.2 * cm,
            rightMargin=1.2 * cm,
            topMargin=3.3 * cm,
            bottomMargin=1.2 * cm,
        )

        styles = getSampleStyleSheet()
        header = ["Detalle", "Boleta", "Plataforma", "Empresa", "Cultivo", "Restricciones", "Neto"]
        table_data = [header]
        row_styles: list[tuple[int, str | None]] = []
        estructura, total_general = self._build_hierarchical_structure(self._rows)

        for cultivo in sorted(estructura.keys()):
            cultivo_node = estructura[cultivo]
            table_data.append(
                [
                    f"CULTIVO: {cultivo}",
                    "",
                    "",
                    "",
                    "",
                    "",
                    self._format_kilos(cultivo_node["total"]),
                ]
            )
            row_styles.append((len(table_data) - 1, "cultivo"))

            for variedad in sorted(cultivo_node["variedades"].keys()):
                variedad_node = cultivo_node["variedades"][variedad]
                table_data.append(
                    [
                        f"   VARIEDAD: {variedad}",
                        "",
                        "",
                        "",
                        "",
                        "",
                        self._format_kilos(variedad_node["total"]),
                    ]
                )
                row_styles.append((len(table_data) - 1, "variedad"))

                for socio in sorted(variedad_node["socios"].keys()):
                    socio_node = variedad_node["socios"][socio]
                    table_data.append(
                        [
                            f"      SOCIO: {socio}",
                            "",
                            "",
                            "",
                            "",
                            "",
                            self._format_kilos(socio_node["total"]),
                        ]
                    )
                    row_styles.append((len(table_data) - 1, "socio"))

                    for albaran, row, kilos in sorted(socio_node["items"], key=lambda item: item[0]):
                        boleta = self._value_or_empty(row.get("Boleta"))
                        table_data.append(
                            [
                                f"         {albaran}",
                                boleta,
                                self._value_or_empty(row.get("Plataforma")),
                                self._value_or_empty(row.get("Empresa")),
                                self._value_or_empty(row.get("Cultivo")),
                                (f"■ {self._value_or_empty(row.get('Color')).upper()}" if self._value_or_empty(row.get('Color')) else ""),
                                self._format_kilos(kilos),
                            ]
                        )
                        color = self._value_or_empty(row.get("Color")).upper()
                        row_styles.append((len(table_data) - 1, color))

        table = Table(
            table_data,
            colWidths=[7.2 * cm, 2.8 * cm, 3.0 * cm, 3.0 * cm, 3.0 * cm, 6.0 * cm, 3.0 * cm],
            repeatRows=1,
        )
        style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#274c77")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                ("ALIGN", (-1, 0), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )

        for row_idx, row_type in row_styles:
            if row_type == "cultivo":
                style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#595959"))
                style.add("TEXTCOLOR", (0, row_idx), (-1, row_idx), colors.white)
                style.add("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold")
            elif row_type == "variedad":
                style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#a6a6a6"))
                style.add("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold")
            elif row_type == "socio":
                style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#d9d9d9"))
                style.add("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold")
            else:
                if row_type in self.COLOR_MAP:
                    style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor(self.COLOR_MAP[row_type]))

        table.setStyle(style)

        total_paragraph = Paragraph(
            f"<b>TOTAL GENERAL: {self._format_kilos(total_general)} kg</b>",
            styles["Heading4"],
        )

        logo_path = self._resolve_logo_for_pdf()

        def draw_page_header(pdf_canvas: Any, pdf_doc: Any) -> None:
            width, height = landscape(A4)
            if logo_path:
                try:
                    pdf_canvas.drawImage(logo_path, 1.2 * cm, height - 2.6 * cm, width=2.8 * cm, height=1.7 * cm, mask="auto")
                except Exception:  # noqa: BLE001
                    pass
            pdf_canvas.setFont("Helvetica-Bold", 15)
            pdf_canvas.drawString(4.3 * cm, height - 1.5 * cm, "Stock de Campo")
            pdf_canvas.setFont("Helvetica", 9)
            pdf_canvas.drawString(
                4.3 * cm,
                height - 2.05 * cm,
                f"Última actualización datos: {self._obtener_ultima_actualizacion()}",
            )
            pdf_canvas.drawRightString(
                width - 1.2 * cm,
                height - 1.5 * cm,
                datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
            )

        story = [Spacer(1, 0.2 * cm), table, Spacer(1, 0.3 * cm), total_paragraph]
        doc.build(story, onFirstPage=draw_page_header, onLaterPages=draw_page_header)


def abrir_stock_campo(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Stock de Campo y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, StockCampoWindow):
            window.lift()
            window.focus_set()
            return

    StockCampoWindow(parent, db)
