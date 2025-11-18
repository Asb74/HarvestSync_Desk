"""Utilities for working with temporary PDF files in HarvestSync Desk."""
from __future__ import annotations

import datetime
import os
import re
import tempfile
import threading
import time
import unicodedata
import webbrowser
from typing import Optional

_pdf_counter = 0


def slugify(text: Optional[str], default: str = "Informe") -> str:
    """Return a filesystem-safe slug derived from *text*."""
    if not text:
        text = default
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.strip()
    text = re.sub(r"[\W]+", "_", text, flags=re.UNICODE)
    return text or default


def create_temp_pdf_name(nombre: Optional[str], prefix: str = "Informe") -> str:
    """Create an absolute path for a unique temporary PDF file."""
    global _pdf_counter
    _pdf_counter += 1

    safe_name = slugify(nombre or prefix, default=prefix)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_name}_{ts}_{_pdf_counter:03d}.pdf"

    temp_dir = os.path.join(tempfile.gettempdir(), "HarvestSyncDesk")
    os.makedirs(temp_dir, exist_ok=True)
    return os.path.join(temp_dir, filename)


def _start_pdf_viewer(filename: str) -> None:
    """Open *filename* with the default PDF viewer, handling fallbacks."""
    try:
        if hasattr(os, "startfile"):
            os.startfile(filename)  # type: ignore[attr-defined]
        else:
            # Fall back to the user's default handler
            webbrowser.open_new(rf"file://{filename}")
    except Exception as exc:  # noqa: BLE001 - best effort opening
        print(f"Error opening PDF '{filename}': {exc}")


def open_and_cleanup_pdf(filename: str, delay: float = 2.0, max_attempts: int = 60) -> None:
    """Open a PDF file and remove it once it's no longer locked."""
    _start_pdf_viewer(filename)

    def _cleanup() -> None:
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            try:
                os.remove(filename)
                print(f"Temporary PDF removed: {filename}")
                return
            except PermissionError:
                time.sleep(delay)
            except FileNotFoundError:
                return
            except Exception as exc:  # noqa: BLE001 - log unexpected issues
                print(f"Error deleting PDF {filename}: {exc}")
                return

    threading.Thread(target=_cleanup, daemon=True).start()
