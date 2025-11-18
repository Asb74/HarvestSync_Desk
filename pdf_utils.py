"""Utilities for working with temporary PDF files in HarvestSync Desk."""
from __future__ import annotations

import datetime
import os
import re
import tempfile
import unicodedata
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


def open_pdf(filename: str) -> None:
    """Open the PDF with the default viewer without deleting it immediately."""
    try:
        os.startfile(filename)  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001 - best effort opening
        print(f"Error opening PDF {filename}: {exc}")


def cleanup_old_pdfs(max_age_hours: int = 24) -> None:
    """Remove temporary PDFs older than *max_age_hours* from the temp folder."""
    base_dir = os.path.join(tempfile.gettempdir(), "HarvestSyncDesk")
    if not os.path.isdir(base_dir):
        return

    now = datetime.datetime.now().timestamp()
    max_age_seconds = max_age_hours * 3600

    for name in os.listdir(base_dir):
        if not name.lower().endswith(".pdf"):
            continue
        path = os.path.join(base_dir, name)
        try:
            stat_info = os.stat(path)
        except FileNotFoundError:
            continue
        except OSError as exc:  # noqa: BLE001 - log unexpected issues
            print(f"Error stating {path}: {exc}")
            continue

        age = now - stat_info.st_mtime
        if age <= max_age_seconds:
            continue

        try:
            os.remove(path)
            print(f"Removed old temp PDF: {path}")
        except PermissionError:
            # File still in use; skip it silently.
            pass
        except FileNotFoundError:
            pass
        except OSError as exc:  # noqa: BLE001 - log unexpected issues
            print(f"Error removing {path}: {exc}")
