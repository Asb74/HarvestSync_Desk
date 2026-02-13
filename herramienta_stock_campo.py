"""Herramienta independiente: Stock de Campo (placeholder)."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from firebase_admin import firestore


class StockCampoWindow(tk.Toplevel):
    """Ventana base para la herramienta de Stock de Campo."""

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent)
        self.db = db_firestore
        self.title("Stock de Campo")
        self.resizable(False, False)
        self._apply_parent_icon(parent)
        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self, padding=20)
        container.pack(fill="both", expand=True)

        ttk.Label(
            container,
            text="Herramienta Stock de Campo",
            font=("Segoe UI", 11, "bold"),
        ).pack(anchor="w")

        ttk.Label(
            container,
            text="Ventana en construcción.",
        ).pack(anchor="w", pady=(8, 0))

    def _apply_parent_icon(self, parent: tk.Widget) -> None:
        """Hereda el icono de la ventana principal si está disponible."""
        try:
            main_window = parent.winfo_toplevel()
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


def abrir_stock_campo(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Stock de Campo y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, StockCampoWindow):
            window.lift()
            window.focus_set()
            return

    StockCampoWindow(parent, db)
