import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


DATA_DIR = Path(os.getenv("WEATHER_DATA_DIR", "/app/data"))
MARKER_FILE = DATA_DIR / ".era5_downloaded"
WIND_CACHE_FILE = DATA_DIR / "open_meteo_wind_hourly.json"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def _build_query_params() -> dict[str, str]:
    return {
        # Kept for backward compatibility only. We no longer pre-download a fixed
        # lat/lon at container startup because wind requests must be derived from
        # each uploaded flight CSV (lat/lon + time range).
        "wind_speed_unit": "ms",
    }


def ensure_meteorological_data() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Wind cache is now created per-run from uploaded CSV (see open_meteo_cache.py).
    # This function keeps the side effect of ensuring /app/data exists.
    return


def start_server() -> None:
    host = os.getenv("UVICORN_HOST", "0.0.0.0")
    port = os.getenv("UVICORN_PORT", "8000")
    timeout_keep_alive = (
        os.getenv("UVICORN_TIMEOUT_KEEP_ALIVE_S")
        or os.getenv("UVICORN_TIMEOUT_KEEP_ALIVE")
        or "120"
    )
    graceful_timeout = (
        os.getenv("UVICORN_GRACEFUL_TIMEOUT_S")
        or os.getenv("UVICORN_GRACEFUL_TIMEOUT")
        or "120"
    )
    os.execvp(
        "uvicorn",
        [
            "uvicorn",
            "main:app",
            "--host",
            host,
            "--port",
            port,
            "--timeout-keep-alive",
            timeout_keep_alive,
            "--timeout-graceful-shutdown",
            graceful_timeout,
        ],
    )


if __name__ == "__main__":
    ensure_meteorological_data()
    start_server()
