"""Utilidades de interfaz para ventanas Tkinter."""
from __future__ import annotations

from typing import Any

import tkinter as tk


def aplicar_icono_principal(window: tk.Toplevel, parent: tk.Widget) -> None:
    """Aplica en ``window`` el icono configurado en la ventana principal."""
    if window is None or parent is None:
        return

    master_candidate: tk.Misc | None = parent
    while master_candidate is not None:
        icon_reference = getattr(master_candidate, "logo_icon", None)
        if icon_reference:
            try:
                window.iconphoto(True, icon_reference)
                return
            except tk.TclError:
                break
        master_candidate = getattr(master_candidate, "master", None)

    try:
        main_window = parent.winfo_toplevel()
    except tk.TclError:
        return

    try:
        current_icon = main_window.iconphoto(False)
        if isinstance(current_icon, (tuple, list)):
            window.iconphoto(False, *current_icon)
        elif current_icon:
            window.iconphoto(False, current_icon)
    except tk.TclError:
        pass


class BaseToolWindow(tk.Toplevel):
    """Clase base para ventanas de herramientas."""

    def __init__(self, parent: tk.Widget, db: Any | None = None, **kwargs: Any) -> None:
        super().__init__(parent, **kwargs)
        self.db = db
        aplicar_icono_principal(self, parent)
