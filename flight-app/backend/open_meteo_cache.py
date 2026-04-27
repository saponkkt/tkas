from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def _parse_utc_series_to_datetime_utc(s: pd.Series) -> pd.Series:
    # Expect ISO like 2026-01-01T12:34:56Z. Keep robust fallbacks.
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    if dt.notna().any():
        return dt
    # Fallback: epoch seconds / ms
    nums = pd.to_numeric(s, errors="coerce")
    if nums.notna().any():
        # Heuristic: >1e12 is probably ms
        if float(nums.max(skipna=True)) > 1e12:
            return pd.to_datetime(nums, unit="ms", errors="coerce", utc=True)
        return pd.to_datetime(nums, unit="s", errors="coerce", utc=True)
    return dt


def ensure_open_meteo_wind_cache_for_csv(
    csv_path: str,
    run_id: str,
    data_dir: str | Path | None = None,
) -> Path | None:
    """
    Derive (lat, lon, start_date, end_date) from the input CSV and cache Open-Meteo hourly wind.
    Non-fatal: returns None on failure.
    """
    out_dir = Path(data_dir or os.getenv("WEATHER_DATA_DIR", "/app/data"))
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_path = out_dir / f"open_meteo_wind_hourly_{run_id}.json"
    marker_path = out_dir / f".open_meteo_cached_{run_id}.json"
    if marker_path.exists() and cache_path.exists():
        return cache_path

    try:
        df = pd.read_csv(csv_path, usecols=lambda c: c in {"UTC", "Latitude", "Longitude"})
        if df.empty:
            return None
        if not {"UTC", "Latitude", "Longitude"}.issubset(set(df.columns)):
            return None

        lat = float(pd.to_numeric(df["Latitude"], errors="coerce").median(skipna=True))
        lon = float(pd.to_numeric(df["Longitude"], errors="coerce").median(skipna=True))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None

        dt = _parse_utc_series_to_datetime_utc(df["UTC"])
        dt = dt.dropna()
        if dt.empty:
            return None

        start_date = dt.min().date().isoformat()
        end_date = dt.max().date().isoformat()

        timeout_s = int(
            os.getenv("OPEN_METEO_TIMEOUT_S")
            or os.getenv("WEATHER_DOWNLOAD_TIMEOUT_S")
            or os.getenv("WEATHER_DOWNLOAD_TIMEOUT")
            or "60"
        )
        timeout = None if timeout_s <= 0 else timeout_s
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "wind_speed_10m,wind_direction_10m",
            "wind_speed_unit": "ms",
            "timezone": "UTC",
        }
        resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()

        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        marker_payload = {
            "source": "open-meteo-archive",
            "url": OPEN_METEO_ARCHIVE_URL,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "cache_file": str(cache_path),
            "derived_from_csv": str(csv_path),
            "query": params,
        }
        with marker_path.open("w", encoding="utf-8") as f:
            json.dump(marker_payload, f, ensure_ascii=False, indent=2)

        return cache_path
    except Exception as exc:
        print(f"[open-meteo][warning] cache failed for run {run_id[:8]}: {exc}")
        return None

