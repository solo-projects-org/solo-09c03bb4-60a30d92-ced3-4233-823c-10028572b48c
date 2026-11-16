"""Runtime configuration for the transit data web application.

All knobs are environment variables so the same image runs in tests,
docker-compose and bare-metal.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get("APP_DB_PATH", "/data/webapp.db")
    gateway_url: str = os.environ.get("GATEWAY_URL", "http://gateway:8001/gtfs-rt")
    sign_url: str = os.environ.get("SIGN_URL", "http://sign:8002")
    poll_interval: float = _float("POLL_INTERVAL", 3.0)
    # HTTP timeout for both gateway and sign calls.
    http_timeout: float = _float("HTTP_TIMEOUT", 5.0)
    # A push attempt that never answers gets retried after this many seconds.
    push_retry_interval: float = _float("PUSH_RETRY_INTERVAL", 3.0)
    # Background worker switch (tests drive the steps manually when False).
    background_workers: bool = os.environ.get("BACKGROUND_WORKERS", "1") == "1"


settings = Settings()
