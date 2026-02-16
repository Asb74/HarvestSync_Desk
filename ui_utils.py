"""Utilidades de interfaz para ventanas Tkinter."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import tkinter as tk

try:
    from PIL import Image, ImageTk
except Exception:  # pragma: no cover - fallback cuando PIL no está disponible
    Image = None
    ImageTk = None


_GLOBAL_ICON_IMAGE: tk.PhotoImage | Any | None = None


def resource_path(rel_path: str) -> str:
    """Devuelve una ruta válida tanto en .py como en ejecutable de PyInstaller."""
    if getattr(sys, "frozen", False):
        app_dir = Path(sys.executable).parent
        direct_path = app_dir / rel_path
        if direct_path.exists():
            return str(direct_path)
        base = Path(getattr(sys, "_MEIPASS", app_dir))
        return str(base / rel_path)
    return str(Path(__file__).resolve().parent / rel_path)


def _load_photo_icon() -> tk.PhotoImage | Any | None:
    for icon_name in ("icono_app.png", "icono_app.ico"):
        icon_path = Path(resource_path(icon_name))
        if not icon_path.exists():
            continue
        try:
            return tk.PhotoImage(file=str(icon_path))
        except tk.TclError:
            if Image is None or ImageTk is None:
                continue
            try:
                return ImageTk.PhotoImage(Image.open(icon_path))
            except Exception:
                continue
    return None


def apply_global_icon(window: tk.Misc) -> tk.PhotoImage | Any | None:
    """Carga y aplica el icono de forma global para Tk y Toplevel."""
    global _GLOBAL_ICON_IMAGE

    if _GLOBAL_ICON_IMAGE is None:
        _GLOBAL_ICON_IMAGE = _load_photo_icon()
    if _GLOBAL_ICON_IMAGE is None:
        return None

    top = window.winfo_toplevel()
    try:
        top.iconphoto(True, _GLOBAL_ICON_IMAGE)
    except tk.TclError:
        return None

    setattr(top, "logo_icon", _GLOBAL_ICON_IMAGE)
    return _GLOBAL_ICON_IMAGE


def aplicar_icono_principal(window: tk.Toplevel, parent: tk.Widget) -> None:
    """Aplica en ``window`` el icono configurado en la ventana principal."""
    if window is None or parent is None:
        return

    apply_global_icon(parent)

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
