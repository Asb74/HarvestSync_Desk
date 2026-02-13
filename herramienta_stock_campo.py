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

import pyodbc
import tkinter as tk
from firebase_admin import firestore
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle
from tkinter import messagebox, ttk

from ui_utils import BaseToolWindow

MDB_PATH = r"X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb"
ACTUALIZACIONES_PATH = Path(r"X:\\Backup Perceco\\actualizaciones.txt")


class StockCampoWindow(BaseToolWindow):
    """Ventana para calcular y exportar el Stock de Campo pendiente de volcar."""

    COLOR_MAP = {
        "ROJO": "#ffd6d6",
        "AMARILLO": "#fff7bf",
        "VERDE": "#d8f5d0",
    }

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

        self.btn_calcular = ttk.Button(frame_filtros, text="🔍 Calcular", command=self._lanzar_calculo)
        self.btn_calcular.grid(row=0, column=0, padx=6, pady=(0, 6), sticky="e")

        self.btn_reset_filtros = ttk.Button(frame_filtros, text="♻ Reiniciar filtros", command=self._reiniciar_filtros)
        self.btn_reset_filtros.grid(row=0, column=1, padx=6, pady=(0, 6), sticky="e")

        self.btn_exportar = ttk.Button(frame_filtros, text="👁 Ver PDF", command=self._exportar_pdf)
        self.btn_exportar.grid(row=0, column=2, padx=6, pady=(0, 6), sticky="e")

        for col in range(3):
            frame_filtros.columnconfigure(col, weight=1)

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

        columns = ("Plataforma", "Empresa", "Cultivo", "Variedad", "Restricciones", "KilosPendientes")
        self.tree = ttk.Treeview(frame_tabla, columns=columns, show="headings", height=18)
        for col in columns:
            anchor = "e" if col == "KilosPendientes" else "w"
            width = 170 if col != "KilosPendientes" else 150
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor=anchor)

        self.tree.tag_configure("ROJO", background=self.COLOR_MAP["ROJO"])
        self.tree.tag_configure("AMARILLO", background=self.COLOR_MAP["AMARILLO"])
        self.tree.tag_configure("VERDE", background=self.COLOR_MAP["VERDE"])

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
        side_sections = ["F. Carga", "Socio", "Variedad", "Color", "Plataforma"]

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

    def _get_connection(self) -> pyodbc.Connection:
        conn_str = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={MDB_PATH};"
        )
        return pyodbc.connect(conn_str, timeout=60)

    def _construir_sql(self) -> tuple[str, list[Any]]:
        params: list[Any] = []

        where_clause = """
            WHERE
                p.AlbaranDef IS NOT NULL
                AND p.AlbaranDef <> ''
                AND NOT EXISTS (
                    SELECT 1
                    FROM Partidas pr
                    WHERE
                        LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida0 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida1 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida2 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida3 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida4 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida5 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida6 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida7 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida8 & '',50)
                        OR LEFT(p.AlbaranDef & '',50) = LEFT(pr.IdPartida9 & '',50)
                )
        """
        sql = f"""
            SELECT
                p.AlbaranDef,
                IIf(IsNull(p.Socio),'',p.Socio) AS Socio,
                p.Fcarga,
                IIf(IsNull(p.Variedad),'',p.Variedad) AS Variedad,
                IIf(IsNull(p.Plataforma),'',p.Plataforma) AS Plataforma,
                IIf(IsNull(p.EMPRESA),'',p.EMPRESA) AS EMPRESA,
                IIf(IsNull(p.CULTIVO),'',p.CULTIVO) AS CULTIVO,
                p.Neto,
                p.NetoPartida,
                IIf(IsNull(p.Restricciones),'',p.Restricciones) AS Restricciones,
                IIf(IsNull(LEFT(m.Valor & '',50)),'',LEFT(m.Valor & '',50)) AS Color
            FROM PesosFres AS p
            LEFT JOIN MRestricciones AS m
                ON LEFT(m.IdRestricciones & '',50) = LEFT(p.Restricciones & '',50)
                AND LEFT(m.CULTIVO & '',50) = LEFT(p.CULTIVO & '',50)
            {where_clause}
        """
        return sql, params

    def _obtener_ultima_actualizacion(self) -> str:
        try:
            if not ACTUALIZACIONES_PATH.exists() or not ACTUALIZACIONES_PATH.is_file():
                return "No disponible"

            lineas = ACTUALIZACIONES_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
            for linea in reversed(lineas):
                contenido = linea.strip()
                if not contenido:
                    continue
                fecha_hora, separador, _ = contenido.partition(" - ")
                if separador and fecha_hora.strip():
                    return fecha_hora.strip()
                return contenido
        except Exception:  # noqa: BLE001
            return "No disponible"
        return "No disponible"

    def _actualizar_label_actualizacion(self) -> None:
        self.actualizacion_var.set(f"Última actualización: {self._obtener_ultima_actualizacion()}")

    def _lanzar_calculo(self, disable_buttons: bool = True) -> None:
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
        sql, params = self._construir_sql()
        print("----- SQL GENERADA -----")
        print(sql)
        print(f"PARAMS: {params}")
        print("------------------------")
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(sql, params)
        data: list[dict[str, Any]] = []
        for row in cur.fetchall():
            neto_partida = 0.0 if row.NetoPartida is None else float(row.NetoPartida)
            neto = 0.0 if row.Neto is None else float(row.Neto)
            kilos = neto_partida if neto_partida != 0 else neto
            fcarga = row.Fcarga
            if isinstance(fcarga, datetime.datetime):
                fcarga_fmt = fcarga.strftime("%d/%m/%Y")
            elif isinstance(fcarga, datetime.date):
                fcarga_fmt = datetime.datetime.combine(fcarga, datetime.time.min).strftime("%d/%m/%Y")
            else:
                fcarga_fmt = "" if fcarga is None else str(fcarga)
            data.append(
                {
                    "AlbaranDef": "" if row.AlbaranDef is None else str(row.AlbaranDef),
                    "Socio": "" if row.Socio is None else str(row.Socio),
                    "Fcarga": fcarga_fmt,
                    "Plataforma": "" if row.Plataforma is None else str(row.Plataforma),
                    "Empresa": "" if row.EMPRESA is None else str(row.EMPRESA),
                    "Cultivo": "" if row.CULTIVO is None else str(row.CULTIVO),
                    "Variedad": "" if row.Variedad is None else str(row.Variedad),
                    "Restricciones": "" if row.Restricciones is None else str(row.Restricciones),
                    "Color": "" if row.Color is None else str(row.Color),
                    "KilosPendientes": kilos,
                }
            )
        conn.close()
        self._raw_rows = data
        return data

    def _mostrar_resultados(self, rows: list[dict[str, Any]]) -> None:
        if self.tree["show"] != "tree headings":
            self.tree.configure(show="tree headings")
            self.tree.heading("#0", text="Detalle")
            self.tree.column("#0", width=260, anchor="w")
        self._populate_side_filters(rows)
        self._apply_side_filters()

    def _populate_side_filters(self, rows: list[dict[str, Any]]) -> None:
        config = {
            "F. Carga": "Fcarga",
            "Socio": "Socio",
            "Variedad": "Variedad",
            "Color": "Color",
            "Plataforma": "Plataforma",
        }
        for section, field in config.items():
            unique_values = sorted({self._value_or_empty(row.get(field)) for row in rows})
            self._render_side_filter_block(section, unique_values, set())

    def _render_side_filter_block(self, section: str, values: list[str], selected_values: set[str]) -> None:
        block = self.side_filter_blocks.get(section)
        if not block:
            return
        for check in block.get("checkbuttons", []):
            check.destroy()
        block["variables"] = []
        block["checkbuttons"] = []
        block["all_values"] = list(values)

        select_all_var = tk.BooleanVar(value=False)

        def on_select_all_change() -> None:
            if self._syncing_filters:
                return
            should_select = select_all_var.get()
            self._syncing_filters = True
            for _, item_var in block["variables"]:
                item_var.set(should_select)
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
                command=self._apply_side_filters,
            )
            check.pack(fill="x", anchor="w")
            block["variables"].append((value, var))
            block["checkbuttons"].append(check)

    def _value_or_empty(self, value: Any) -> str:
        return "" if value is None else str(value).strip()

    def _get_selected_values(self, section: str) -> set[str]:
        block = self.side_filter_blocks.get(section, {})
        variables = block.get("variables", [])
        selected = {val for val, var in variables if var.get()}
        if selected:
            return selected
        return set(block.get("all_values", []))

    def _apply_side_filters(self) -> None:
        if self._syncing_filters:
            return
        raw_rows = getattr(self, "_raw_rows", [])
        filter_config = {
            "F. Carga": "Fcarga",
            "Socio": "Socio",
            "Variedad": "Variedad",
            "Color": "Color",
            "Plataforma": "Plataforma",
        }

        selected_by_section = {section: self._get_selected_values(section) for section in filter_config}
        filtered_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            include = True
            for section, field in filter_config.items():
                if self._value_or_empty(row.get(field)) not in selected_by_section[section]:
                    include = False
                    break
            if include:
                filtered_rows.append(row)
        self._rows = filtered_rows

        available_values = {
            section: sorted({self._value_or_empty(row.get(field)) for row in filtered_rows})
            for section, field in filter_config.items()
        }

        for section in filter_config:
            current_selected = {val for val, var in self.side_filter_blocks.get(section, {}).get("variables", []) if var.get()}
            kept_selected = {val for val in current_selected if val in set(available_values[section])}
            self._render_side_filter_block(section, available_values[section], kept_selected)

        self._render_tree_rows(filtered_rows)

    def _render_tree_rows(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        estructura, total_general = self._build_hierarchical_structure(rows)
        for variedad in sorted(estructura.keys()):
            variedad_node = estructura[variedad]
            variedad_iid = self.tree.insert(
                "",
                "end",
                text=variedad,
                values=("", "", "", "", "", self._format_kilos(variedad_node["total"])),
                open=True,
            )
            for socio in sorted(variedad_node["socios"].keys()):
                socio_node = variedad_node["socios"][socio]
                socio_iid = self.tree.insert(
                    variedad_iid,
                    "end",
                    text=socio,
                    values=("", "", "", "", "", self._format_kilos(socio_node["total"])),
                    open=True,
                )
                for albaran, row, kilos in sorted(socio_node["items"], key=lambda item: item[0]):
                    restr = self._value_or_empty(row.get("Restricciones")).upper()
                    tag = ""
                    if "ROJO" in restr:
                        tag = "ROJO"
                    elif "AMARILLO" in restr:
                        tag = "AMARILLO"
                    elif "VERDE" in restr:
                        tag = "VERDE"
                    self.tree.insert(
                        socio_iid,
                        "end",
                        text=albaran,
                        values=(
                            self._value_or_empty(row.get("Plataforma")),
                            self._value_or_empty(row.get("Empresa")),
                            self._value_or_empty(row.get("Cultivo")),
                            self._value_or_empty(row.get("Variedad")),
                            self._value_or_empty(row.get("Restricciones")),
                            self._format_kilos(kilos),
                        ),
                        tags=(tag,) if tag else (),
                    )
        total_fmt = self._format_kilos(total_general)
        self.total_general_var.set(f"TOTAL GENERAL: {total_fmt} kg")

    def _build_hierarchical_structure(self, rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], float]:
        estructura: dict[str, dict[str, Any]] = {}
        total_general = 0.0
        for row in rows:
            variedad = self._value_or_empty(row.get("Variedad")) or "(Sin variedad)"
            socio = self._value_or_empty(row.get("Socio")) or "(Sin socio)"
            albaran = self._value_or_empty(row.get("AlbaranDef")) or "(Sin albarán)"
            kilos = float(row.get("KilosPendientes") or 0.0)
            total_general += kilos
            if variedad not in estructura:
                estructura[variedad] = {"total": 0.0, "socios": {}}
            estructura[variedad]["total"] += kilos
            socios = estructura[variedad]["socios"]
            if socio not in socios:
                socios[socio] = {"total": 0.0, "items": []}
            socios[socio]["total"] += kilos
            socios[socio]["items"].append((albaran, row, kilos))
        return estructura, total_general

    def _reiniciar_filtros(self) -> None:
        self._syncing_filters = True
        for block in self.side_filter_blocks.values():
            for _, var in block.get("variables", []):
                var.set(False)
            select_all_var = block.get("select_all_var")
            if select_all_var is not None:
                select_all_var.set(False)
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
        c = canvas.Canvas(filename, pagesize=landscape(A4))
        width, height = landscape(A4)

        logo_path = self._resolve_logo_for_pdf()
        if logo_path:
            try:
                c.drawImage(logo_path, 1.4 * cm, height - 2.7 * cm, width=2.8 * cm, height=1.7 * cm, mask="auto")
            except Exception:  # noqa: BLE001
                pass

        c.setFont("Helvetica-Bold", 16)
        c.drawString(4.6 * cm, height - 1.7 * cm, "Stock de Campo")
        c.setFont("Helvetica", 10)
        c.drawString(
            4.6 * cm,
            height - 2.25 * cm,
            f"Última actualización datos: {self._obtener_ultima_actualizacion()}",
        )
        c.setFont("Helvetica", 10)
        c.drawRightString(width - 1.5 * cm, height - 1.7 * cm, datetime.datetime.now().strftime("%d/%m/%Y %H:%M"))

        header = ["Detalle", "Plataforma", "Empresa", "Cultivo", "Restricciones", "KilosPendientes"]
        table_data = [header]
        row_styles: list[tuple[int, str | None]] = []
        estructura, total_general = self._build_hierarchical_structure(self._rows)

        for variedad in sorted(estructura.keys()):
            variedad_node = estructura[variedad]
            table_data.append(
                [
                    f"VARIEDAD: {variedad}",
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
                        f"   SOCIO: {socio}",
                        "",
                        "",
                        "",
                        "",
                        self._format_kilos(socio_node["total"]),
                    ]
                )
                row_styles.append((len(table_data) - 1, "socio"))

                for albaran, row, kilos in sorted(socio_node["items"], key=lambda item: item[0]):
                    table_data.append(
                        [
                            f"      {albaran}",
                            self._value_or_empty(row.get("Plataforma")),
                            self._value_or_empty(row.get("Empresa")),
                            self._value_or_empty(row.get("Cultivo")),
                            self._value_or_empty(row.get("Restricciones")),
                            self._format_kilos(kilos),
                        ]
                    )
                    restr = self._value_or_empty(row.get("Restricciones")).upper()
                    row_styles.append((len(table_data) - 1, restr))

        table = Table(
            table_data,
            colWidths=[8.1 * cm, 3.2 * cm, 3.2 * cm, 3.2 * cm, 6.5 * cm, 3.2 * cm],
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
                ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )

        for row_idx, row_type in row_styles:
            if row_type == "variedad":
                style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#595959"))
                style.add("TEXTCOLOR", (0, row_idx), (-1, row_idx), colors.white)
                style.add("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold")
            elif row_type == "socio":
                style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#d9d9d9"))
                style.add("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold")
            else:
                if "ROJO" in row_type:
                    style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor(self.COLOR_MAP["ROJO"]))
                elif "AMARILLO" in row_type:
                    style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor(self.COLOR_MAP["AMARILLO"]))
                elif "VERDE" in row_type:
                    style.add("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor(self.COLOR_MAP["VERDE"]))

        table.setStyle(style)
        table.wrapOn(c, width - 3 * cm, height - 7.8 * cm)
        table.drawOn(c, 1.3 * cm, 2.7 * cm)

        c.setFont("Helvetica-Bold", 11)
        c.drawRightString(width - 1.5 * cm, 1.6 * cm, f"TOTAL GENERAL: {self._format_kilos(total_general)} kg")
        c.save()


def abrir_stock_campo(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Stock de Campo y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, StockCampoWindow):
            window.lift()
            window.focus_set()
            return

    StockCampoWindow(parent, db)
