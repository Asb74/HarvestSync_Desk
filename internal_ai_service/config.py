"""Configuración del servicio interno de IA."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_KEY_PATH = Path(r"C:\ProgramData\HarvestSync\Secret\openai_key.txt")


@dataclass(frozen=True)
class Settings:
    bind_host: str
    bind_port: int
    openai_key_path: Path
    openai_model: str
    openai_timeout_seconds: int
    openai_base_url: str
    internal_token: str
    allowed_client_ips: tuple[str, ...]


def _parse_ip_whitelist(raw_value: str) -> tuple[str, ...]:
    items = [part.strip() for part in raw_value.split(",") if part.strip()]
    return tuple(items)


def load_settings() -> Settings:
    """Carga configuración desde entorno con defaults conservadores."""
    bind_host = os.getenv("HARVESTSYNC_AI_BIND_HOST", "0.0.0.0")
    bind_port = int(os.getenv("HARVESTSYNC_AI_BIND_PORT", "8086"))

    key_path_str = os.getenv("HARVESTSYNC_OPENAI_KEY_PATH", str(DEFAULT_KEY_PATH))
    openai_key_path = Path(key_path_str)

    openai_model = os.getenv("HARVESTSYNC_OPENAI_MODEL", "gpt-4.1-mini")
    openai_timeout_seconds = int(os.getenv("HARVESTSYNC_OPENAI_TIMEOUT", "25"))
    openai_base_url = os.getenv("HARVESTSYNC_OPENAI_BASE_URL", "https://api.openai.com/v1")

    internal_token = os.getenv("HARVESTSYNC_INTERNAL_TOKEN", "").strip()
    allowed_client_ips = _parse_ip_whitelist(os.getenv("HARVESTSYNC_ALLOWED_IPS", ""))

    return Settings(
        bind_host=bind_host,
        bind_port=bind_port,
        openai_key_path=openai_key_path,
        openai_model=openai_model,
        openai_timeout_seconds=openai_timeout_seconds,
        openai_base_url=openai_base_url,
        internal_token=internal_token,
        allowed_client_ips=allowed_client_ips,
    )
