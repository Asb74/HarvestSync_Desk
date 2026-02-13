"""Herramienta independiente: Stock de Campo (placeholder)."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from firebase_admin import firestore

from ui_utils import BaseToolWindow


class StockCampoWindow(BaseToolWindow):
    """Ventana base para la herramienta de Stock de Campo."""

    def __init__(self, parent: tk.Widget, db_firestore: firestore.Client) -> None:
        super().__init__(parent, db_firestore)
        self.title("Stock de Campo")
        self.resizable(False, False)
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


def abrir_stock_campo(parent: tk.Widget, db: firestore.Client) -> None:
    """Abre la ventana de Stock de Campo y evita duplicados."""
    root = parent.winfo_toplevel()
    for window in root.winfo_children():
        if isinstance(window, StockCampoWindow):
            window.lift()
            window.focus_set()
            return

    StockCampoWindow(parent, db)
