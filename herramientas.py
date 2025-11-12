"""Herramientas window for HarvestSync Desk."""
from __future__ import annotations

import datetime
import threading
from typing import Any, Dict, Optional

import pyodbc
import tkinter as tk
from tkinter import ttk
from firebase_admin import firestore


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> Optional[int]:
    if value in (None, "", " "):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        if isinstance(value, (int,)):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        return int(float(text.replace(",", ".")))
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", " "):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip().replace(",", ".")
        if not text:
            return None
        return float(text)
    except (ValueError, TypeError):
        return None


def _fmt_date(value: Any) -> Optional[str]:
    if value in (None, "", " "):
        return None

    dt_value: Optional[datetime.datetime] = None

    if isinstance(value, datetime.datetime):
        dt_value = value
    elif isinstance(value, datetime.date):
        dt_value = datetime.datetime.combine(value, datetime.datetime.min.time())
    elif isinstance(value, (int, float)):
        # Access stores dates as OLE Automation date numbers (days since 1899-12-30)
        try:
            base_date = datetime.datetime(1899, 12, 30)
            dt_value = base_date + datetime.timedelta(days=float(value))
        except (OverflowError, ValueError):
            dt_value = None
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                dt_value = datetime.datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt_value is None and text.isdigit() and len(text) == 8:
            try:
                dt_value = datetime.datetime.strptime(text, "%Y%m%d")
            except ValueError:
                dt_value = None
    if dt_value is None:
        return None
    return dt_value.strftime("%d/%m/%Y")


def _safe_after(widget: tk.Widget, callback) -> None:
    if widget is None:
        return
    try:
        widget.after(0, callback)
    except tk.TclError:
        pass


def actualizar_eepp_desde_access(
    db_fs: firestore.Client,
    mdb_path: str,
    table: str,
    log_widget: tk.Text,
    progress_widget: ttk.Progressbar,
) -> None:
    """Synchronize EEPP collection from an Access database table."""

    def log_message(message: str) -> None:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {message}\n"

        def append() -> None:
            try:
                log_widget.configure(state="normal")
                log_widget.insert("end", formatted)
                log_widget.see("end")
                log_widget.configure(state="disabled")
            except tk.TclError:
                pass

        _safe_after(log_widget, append)

    def reset_progress(maximum: int) -> None:
        def setter() -> None:
            try:
                progress_widget.configure(maximum=maximum if maximum > 0 else 1, value=0)
            except tk.TclError:
                pass

        _safe_after(progress_widget, setter)

    def update_progress(value: int) -> None:
        def setter() -> None:
            try:
                progress_widget.configure(value=value)
            except tk.TclError:
                pass

        _safe_after(progress_widget, setter)

    log_message("Iniciando sincronización de EEPP desde Access...")

    try:
        connection_string = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={mdb_path};"
        )
        conn = pyodbc.connect(connection_string, timeout=60)
    except pyodbc.Error as exc:
        log_message(f"Error al conectar con Access: {exc}")
        return

    try:
        cursor = conn.cursor()
        try:
            count_query = f"SELECT COUNT(*) FROM [{table}]"
            total_rows = cursor.execute(count_query).fetchone()[0]
        except pyodbc.Error as exc:
            log_message(f"No se pudo obtener el total de filas: {exc}")
            total_rows = 0

        reset_progress(total_rows)

        try:
            data_query = f"SELECT * FROM [{table}]"
            cursor.execute(data_query)
        except pyodbc.Error as exc:
            log_message(f"No se pudo leer la tabla: {exc}")
            return

        columns = [col[0] for col in cursor.description]
        collection_ref = db_fs.collection("EEPP")
        batch = db_fs.batch()
        batch_size = 0
        processed = 0
        created = 0
        updated = 0
        skipped = 0
        errors = 0

        for row_index, row in enumerate(cursor, start=1):
            processed += 1
            row_dict: Dict[str, Any] = {columns[i]: row[i] for i in range(len(columns))}

            boleta_value = _to_str(row_dict.get("Boleta"))
            if not boleta_value:
                skipped += 1
                log_message(f"Fila {row_index} sin Boleta → omitida")
                update_progress(processed)
                continue

            doc_ref = collection_ref.document(boleta_value)

            try:
                existing_doc = doc_ref.get()
                exists = existing_doc.exists
            except Exception as exc:  # pylint: disable=broad-except
                errors += 1
                log_message(f"No se pudo verificar documento {boleta_value}: {exc}")
                update_progress(processed)
                continue

            data: Dict[str, Any] = {}

            def set_field(field_name: str, value: Any) -> None:
                if value is not None:
                    data[field_name] = value

            set_field("IdSocio", _to_int(row_dict.get("IdSocio")))
            set_field("NIF", _to_str(row_dict.get("NIF")))
            set_field("Nombre", _to_str(row_dict.get("Nombre")))
            set_field("Poligono", _to_str(row_dict.get("Pol")))
            set_field("Parcela", _to_str(row_dict.get("Par")))
            set_field("Recinto", _to_str(row_dict.get("Recinto")))
            set_field("ReTn", _to_float(row_dict.get("ReTn")))
            set_field("SupCul", _to_float(row_dict.get("SupCul")))
            set_field("SupCas", _to_float(row_dict.get("SupCas")))
            set_field("Arbol", _to_int(row_dict.get("Arbol")))
            sub_cultivo = _to_str(row_dict.get("SubCultivo"))
            set_field("SubCultivo", sub_cultivo)
            set_field("Variedad", _to_str(row_dict.get("Variedad")))
            set_field("Tipo", _to_str(row_dict.get("Tipo")))
            set_field("KGTotales", _to_int(row_dict.get("KGTotales")))
            set_field("ParcelaNombre", _to_str(row_dict.get("Parcela")))
            tecnico_value = _to_str(row_dict.get("Tecnico")) or _to_str(row_dict.get("Aplicador"))
            set_field("Tecnico", tecnico_value)
            set_field("NºMaquinas", _to_int(row_dict.get("NºMaquinas")))
            set_field("NºPulv", _to_int(row_dict.get("NºPulv")))
            set_field("Carne", _to_str(row_dict.get("Carne")))
            set_field("Año", _to_int(row_dict.get("Año")))
            set_field("CULTIVO", _to_str(row_dict.get("CULTIVO")))
            set_field("CAMPAÑA", _to_int(row_dict.get("CAMPAÑA")))
            set_field("EMPRESA", _to_int(row_dict.get("EMPRESA")))
            set_field("Altura", _to_str(row_dict.get("Altura")))
            set_field("Vegetacion", _to_str(row_dict.get("Vegetacion")))
            set_field("Densidad", _to_float(row_dict.get("Densidad")))
            set_field("Zona", _to_str(row_dict.get("Zona")))
            set_field("Grupo", _to_str(row_dict.get("Grupo")))
            set_field("Certificacion", _to_str(row_dict.get("Certificacion")))
            set_field("Agrupacion", _to_str(row_dict.get("Agrupacion")))
            set_field("TipoSocio", _to_str(row_dict.get("TipoSocio")))
            set_field("ALTA", _fmt_date(row_dict.get("ALTA")))
            set_field("BAJA", _fmt_date(row_dict.get("BAJA")))
            set_field("FechaPlantacion", _fmt_date(row_dict.get("FechaPlantacion")))
            set_field("FechaRecoleccion", _fmt_date(row_dict.get("FechaRecoleccion")))
            cha_value = row_dict.get("CHA")
            if isinstance(cha_value, (int, float)):
                set_field("CHA", bool(cha_value))
            elif isinstance(cha_value, str):
                stripped = cha_value.strip()
                if stripped in {"0", "1"}:
                    set_field("CHA", stripped == "1")
                elif stripped:
                    set_field("CHA", stripped)
            elif cha_value is not None:
                set_field("CHA", cha_value)
            set_field("NivelGlobal", _to_str(row_dict.get("NivelGlobal")))
            set_field("PI", _to_str(row_dict.get("PI")))
            if sub_cultivo is not None:
                set_field("SubCul", sub_cultivo)

            try:
                batch.set(doc_ref, data, merge=True)
                batch_size += 1
                if exists:
                    updated += 1
                else:
                    created += 1
            except Exception as exc:  # pylint: disable=broad-except
                errors += 1
                log_message(f"Error al preparar documento {boleta_value}: {exc}")

            if batch_size >= 400:
                try:
                    batch.commit()
                    log_message("Commit de 400 docs…")
                except Exception as exc:  # pylint: disable=broad-except
                    errors += 1
                    log_message(f"Error al hacer commit del batch: {exc}")
                finally:
                    batch = db_fs.batch()
                    batch_size = 0

            update_progress(processed)

        if batch_size:
            try:
                batch.commit()
                log_message("Commit final de documentos pendientes…")
            except Exception as exc:  # pylint: disable=broad-except
                errors += 1
                log_message(f"Error en el commit final: {exc}")

        summary = (
            "Resumen sincronización → "
            f"creados: {created}, "
            f"actualizados: {updated}, "
            f"omitidos: {skipped}, "
            f"errores: {errors}, "
            f"total procesado: {processed}"
        )
        log_message(summary)
        update_progress(processed)
        log_message("Sincronización completada.")
    finally:
        conn.close()


class HerramientasWindow(tk.Toplevel):
    """Top-level window that exposes synchronization tools."""

    def __init__(self, master: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(master)
        self.db = db_firestore
        self.title("Herramientas")
        self.resizable(True, False)

        self.path_var = tk.StringVar(
            value=r"X:\\ENLACES\\Power BI\\Campaña\\PercecoBi(Campaña).mdb"
        )
        self.table_var = tk.StringVar(value="DEEPP")

        self._build_ui()

    def _build_ui(self) -> None:
        frame_origen = ttk.LabelFrame(self, text="Origen (Access)")
        frame_origen.pack(fill="x", padx=12, pady=10)

        ttk.Label(frame_origen, text="Ruta del archivo:").grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        entry_path = ttk.Entry(frame_origen, textvariable=self.path_var, width=70)
        entry_path.grid(row=0, column=1, padx=5, pady=5, sticky="we")

        ttk.Label(frame_origen, text="Tabla:").grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        entry_table = ttk.Entry(frame_origen, textvariable=self.table_var, width=30)
        entry_table.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        self.actualizar_btn = ttk.Button(
            frame_origen,
            text="⬆️ Actualizar EEPP",
            command=self._on_actualizar,
        )
        self.actualizar_btn.grid(row=0, column=2, rowspan=2, padx=10, pady=5)

        frame_origen.columnconfigure(1, weight=1)

        progress_frame = ttk.Frame(self)
        progress_frame.pack(fill="x", padx=12, pady=(0, 10))

        self.progress = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x", expand=True)

        log_frame = ttk.Frame(self)
        log_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        scrollbar = ttk.Scrollbar(log_frame)
        scrollbar.pack(side="right", fill="y")

        self.log_text = tk.Text(
            log_frame,
            height=12,
            state="disabled",
            wrap="word",
            background="white",
        )
        self.log_text.pack(side="left", fill="both", expand=True)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.configure(command=self.log_text.yview)

    def _on_actualizar(self) -> None:
        mdb_path = self.path_var.get().strip()
        table = self.table_var.get().strip() or "EEPP"

        if not mdb_path:
            self._log_direct("Debe especificar la ruta del archivo de Access.")
            return

        self.actualizar_btn.config(state="disabled")
        self._log_direct("Iniciando tarea en segundo plano…")

        thread = threading.Thread(
            target=self._run_sync,
            args=(mdb_path, table),
            daemon=True,
        )
        thread.start()

    def _run_sync(self, mdb_path: str, table: str) -> None:
        try:
            actualizar_eepp_desde_access(
                self.db,
                mdb_path,
                table,
                self.log_text,
                self.progress,
            )
        finally:
            try:
                self.after(0, lambda: self.actualizar_btn.config(state="normal"))
            except tk.TclError:
                pass

    def _log_direct(self, message: str) -> None:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {message}\n"
        try:
            self.log_text.configure(state="normal")
            self.log_text.insert("end", formatted)
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        except tk.TclError:
            pass


def abrir_herramientas(root: tk.Widget, db_firestore: firestore.Client) -> None:
    """Open the tools window, reusing it if already visible."""
    for window in root.winfo_children():
        if isinstance(window, HerramientasWindow):
            window.lift()
            window.focus_set()
            return

    HerramientasWindow(root, db_firestore)
