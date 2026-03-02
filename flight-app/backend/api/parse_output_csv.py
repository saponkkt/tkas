"""
Parse output CSV from process_adsb_pipeline.py.
Splits time-series rows from summary rows (ETOW, Total_Fuel, Trip_fuel, Total_CO2).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


SUMMARY_KEYS = ("ETOW", "Total_Fuel", "Trip_fuel", "Total_CO2")


def _normalize_col(df: pd.DataFrame, *candidates: str) -> str | None:
    existing = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        if c.lower() in existing:
            return existing[c.lower()]
    return None


def parse_output_csv(path: str | Path) -> tuple[pd.DataFrame, dict[str, float]]:
    """
    Read the pipeline output CSV. Return (data_df, summary_dict).
    - data_df: time-series rows only (no summary footer).
    - summary_dict: keys ETOW, Total_Fuel, Trip_fuel, Total_CO2 (values as float).
    """
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(), {}

    # Read full file; summary rows have only 2 columns or first column in SUMMARY_KEYS
    df = pd.read_csv(path)

    # Detect summary rows: first column (by name or position) is one of SUMMARY_KEYS
    first_col = df.columns[0]
    mask = df[first_col].astype(str).str.strip().str.upper().isin([k.upper() for k in SUMMARY_KEYS])
    summary_rows = df[mask]

    summary_dict: dict[str, float] = {}
    for _, row in summary_rows.iterrows():
        key = str(row.iloc[0]).strip()
        for sk in SUMMARY_KEYS:
            if key.upper() == sk.upper():
                try:
                    val = float(row.iloc[1])
                    summary_dict[sk] = val
                except (ValueError, TypeError):
                    pass
                break

    data_df = df[~mask].copy()
    # Drop rows that are all NaN (e.g. blank separator line)
    data_df = data_df.dropna(how="all")
    # Reset index for clean iteration
    data_df = data_df.reset_index(drop=True)

    return data_df, summary_dict


def data_to_track_rows(data_df: pd.DataFrame) -> list[dict]:
    """Convert data DataFrame to list of dicts for flight_track insert."""
    if data_df is None or len(data_df) == 0:
        return []

    ts_col = _normalize_col(data_df, "UTC", "timestamp", "utc_time") or "UTC"
    lat_col = _normalize_col(data_df, "latitude", "lat") or "latitude"
    lon_col = _normalize_col(data_df, "longitude", "lon") or "longitude"
    alt_col = _normalize_col(data_df, "altitude", "alt") or "altitude"
    speed_col = _normalize_col(data_df, "TAS_kt", "Speed", "speed", "tas_kt")
    phase_col = _normalize_col(data_df, "flight_phase") or "flight_phase"

    rows = []
    for _, r in data_df.iterrows():
        try:
            ts = r.get(ts_col)
            if pd.isna(ts):
                continue
            ts_str = pd.Timestamp(ts).isoformat() if hasattr(ts, "isoformat") else str(ts)
            lat = float(r[lat_col]) if lat_col in data_df.columns and not pd.isna(r.get(lat_col)) else None
            lon = float(r[lon_col]) if lon_col in data_df.columns and not pd.isna(r.get(lon_col)) else None
            if lat is None or lon is None:
                continue
            alt = float(r[alt_col]) if alt_col in data_df.columns and not pd.isna(r.get(alt_col)) else None
            speed = float(r[speed_col]) if speed_col and speed_col in data_df.columns and not pd.isna(r.get(speed_col)) else None
            phase = str(r[phase_col]).strip() if phase_col and phase_col in data_df.columns and not pd.isna(r.get(phase_col)) else None
            rows.append({
                "timestamp": ts_str,
                "latitude": lat,
                "longitude": lon,
                "altitude": alt,
                "speed": speed,
                "flight_phase": phase,
            })
        except (ValueError, TypeError, KeyError):
            continue
    return rows


def data_to_segment_rows(data_df: pd.DataFrame) -> list[dict]:
    """Convert data DataFrame to list of dicts for flight_segment insert."""
    if data_df is None or len(data_df) == 0:
        return []

    ts_col = _normalize_col(data_df, "UTC", "timestamp", "utc_time") or "UTC"
    dt_col = None
    for c in data_df.columns:
        if "delta" in c.lower() and "s" in c.lower():
            dt_col = c
            break
    if dt_col is None:
        dt_col = "delta_t (s)"
    fuel_col = _normalize_col(data_df, "Fuel_at_time_TE", "Fuel_at_time_kg", "fuel_at_time")
    co2_col = _normalize_col(data_df, "CO2_at_time_TE", "CO2_at_time", "co2_at_time")

    rows = []
    for _, r in data_df.iterrows():
        try:
            ts = r.get(ts_col)
            if pd.isna(ts):
                continue
            ts_str = pd.Timestamp(ts).isoformat() if hasattr(ts, "isoformat") else str(ts)
            delta_t = float(r[dt_col]) if dt_col in data_df.columns and not pd.isna(r.get(dt_col)) else None
            fuel = float(r[fuel_col]) if fuel_col and fuel_col in data_df.columns and not pd.isna(r.get(fuel_col)) else None
            co2 = float(r[co2_col]) if co2_col and co2_col in data_df.columns and not pd.isna(r.get(co2_col)) else None
            rows.append({
                "timestamp": ts_str,
                "delta_t_s": delta_t,
                "fuel_kg": fuel,
                "co2_kg": co2,
            })
        except (ValueError, TypeError, KeyError):
            continue
    return rows
