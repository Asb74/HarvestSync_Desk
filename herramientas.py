"""Menú contenedor de herramientas para HarvestSync Desk."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from firebase_admin import firestore

from herramienta_actualizar_efectivo import abrir_actualizar_efectivo
from herramienta_stock_campo import abrir_stock_campo


class HerramientasWindow(tk.Toplevel):
    """Ventana contenedora para lanzar herramientas independientes."""

    def __init__(self, master: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(master)
        self.db = db_firestore
        self.title("Herramientas")
        self.resizable(False, False)
        self._apply_master_icon(master)
        self._build_ui()

    def _build_ui(self) -> None:
        contenedor = ttk.Frame(self, padding=16)
        contenedor.pack(fill="both", expand=True)

        ttk.Label(
            contenedor,
            text="Selecciona una herramienta:",
            font=("Segoe UI", 11, "bold"),
        ).pack(anchor="w", pady=(0, 10))

        ttk.Button(
            contenedor,
            text="⬆️ Actualizar Efectivo Productivo",
            command=lambda: abrir_actualizar_efectivo(self, self.db),
        ).pack(fill="x", pady=4)

        ttk.Button(
            contenedor,
            text="🌱 Stock de Campo",
            command=lambda: abrir_stock_campo(self, self.db),
        ).pack(fill="x", pady=4)

    def _apply_master_icon(self, master: tk.Widget) -> None:
        """Hereda el icono configurado en la ventana principal si existe."""
        if master is None:
            return

        try:
            main_window = master.winfo_toplevel()
        except tk.TclError:
            return

        icon_reference = getattr(main_window, "logo_icon", None)
        if icon_reference:
            try:
                self.iconphoto(False, icon_reference)
                return
            except tk.TclError:
                pass

        try:
            current_icon = main_window.iconphoto(False)
            if isinstance(current_icon, (tuple, list)):
                self.iconphoto(False, *current_icon)
            elif current_icon:
                self.iconphoto(False, current_icon)
        except tk.TclError:
            pass



def abrir_herramientas(root: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de herramientas y evita duplicados."""
    for window in root.winfo_children():
        if isinstance(window, HerramientasWindow):
            window.lift()
            window.focus_set()
            return

    HerramientasWindow(root, db)
