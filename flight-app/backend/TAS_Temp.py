from __future__ import annotations

import os
from io import IOBase
from typing import BinaryIO, Dict, Optional, Union

import math

import numpy as np
import pandas as pd
import xarray as xr

from haversine import haversine_nm


# ============================================================================
# Services สำหรับ endpoint /upload (ระยะทาง, fuel, mass, CO2)
# ============================================================================


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    ตรวจสอบให้แน่ใจว่า CSV มีคอลัมน์ที่ต้องใช้: lat, lon, altitude, timestamp
    และจัดการ dtype ให้เหมาะสม
    """
    # Work with a copy
    df = df.copy()

    # Helper: map possible alternative column names to canonical names
    col_map = {}
    lower_cols = {c.lower(): c for c in df.columns}

    def find_col(candidates):
        for cand in candidates:
            lc = cand.lower()
            if lc in lower_cols:
                return lower_cols[lc]
        return None

    # latitude/longitude: support 'lat','latitude' or 'Position' ("lat,lon")
    lat_col = find_col(["lat", "latitude", "lat_deg", "latitude_deg"])
    lon_col = find_col(["lon", "longitude", "lon_deg", "longitude_deg"])
    pos_col = find_col(["position", "pos"])  # e.g. "12.34,98.76"

    if lat_col is None or lon_col is None:
        # try to parse Position if present
        if pos_col is not None:
            # attempt splitting by common separators
            pos = df[pos_col].astype(str)
            split = pos.str.split(
                r"[,;|\s]+", expand=True, regex=True
            )
            if split.shape[1] >= 2:
                df["lat"] = pd.to_numeric(split[0], errors="coerce")
                df["lon"] = pd.to_numeric(split[1], errors="coerce")
                lat_col = "lat"
                lon_col = "lon"

    # If we found specific lat/lon columns, copy/convert to canonical names
    if lat_col is not None:
        df["lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    if lon_col is not None:
        df["lon"] = pd.to_numeric(df[lon_col], errors="coerce")

    # altitude: accept many names
    alt_col = find_col(["altitude", "alt", "height", "alt_ft", "altitude_ft"])
    if alt_col is not None:
        df["altitude"] = pd.to_numeric(df[alt_col], errors="coerce")

    # timestamp/time: accept several names and make timezone-aware UTC if possible
    ts_col = find_col(["timestamp", "time", "utc", "datetime", "date"])
    if ts_col is not None:
        try:
            df["timestamp"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        except Exception:
            df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")

    # Final validation: must have lat, lon, timestamp
    missing = [c for c in ["lat", "lon", "timestamp"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after normalization: {', '.join(missing)}")

    # Drop rows with missing essential values
    df = df.dropna(subset=["lat", "lon", "timestamp"])

    # Ensure numeric altitude column exists (may be NaN if not present)
    if "altitude" not in df.columns:
        df["altitude"] = pd.NA
    else:
        df["altitude"] = pd.to_numeric(df["altitude"], errors="coerce")

    # Normalize column order and return
    return df.reset_index(drop=True)


'''def compute_flight_metrics_from_csv(
    file_obj: Union[BinaryIO, IOBase]
) -> Dict[str, float]:
    """
    อ่าน FlightRadar24-like CSV และคำนวณ distance, fuel, mass, CO2
    ใช้สำหรับ backend FastAPI /upload
    """
    df = pd.read_csv(file_obj)
    df = _ensure_required_columns(df)

    if len(df) < 2:
        # Not enough points to form a segment
        return {
            "distance_nm": 0.0,
            "fuel_kg": 0.0,
            "mass_kg": 0.0,
            "co2_kg": 0.0,
        }

    # Sort by timestamp to get real path order if not already sorted
    df = df.sort_values("timestamp").reset_index(drop=True)

    total_distance_nm = 0.0
    for i in range(1, len(df)):
        lat1, lon1 = float(df.loc[i - 1, "lat"]), float(df.loc[i - 1, "lon"])
        lat2, lon2 = float(df.loc[i, "lat"]), float(df.loc[i, "lon"])
        total_distance_nm += haversine_nm(lat1, lon1, lat2, lon2)

    fuel_kg = total_distance_nm * 4.2
    mass_kg = fuel_kg * 1.05
    co2_kg = fuel_kg * 3.16

    return {
        "distance_nm": round(total_distance_nm, 3),
        "fuel_kg": round(fuel_kg, 3),
        "mass_kg": round(mass_kg, 3),
        "co2_kg": round(co2_kg, 3),
    }'''

    # Utility: parse a Position column like "lat,lon" into latitude/longitude
def parse_position_column(df: pd.DataFrame, pos_col: str = "Position") -> pd.DataFrame:
        """If `pos_col` exists and lat/lon are missing, parse it into `latitude`/`longitude`.

        Supports separators: comma, semicolon, pipe or whitespace.
        """
        if pos_col not in df.columns:
            return df

        pos = df[pos_col].astype(str)
        split = pos.str.split(r"[,;|\s]+", expand=True, regex=True)
        if split.shape[1] >= 2:
            out = df.copy()
            out["latitude"] = pd.to_numeric(split[0], errors="coerce")
            out["longitude"] = pd.to_numeric(split[1], errors="coerce")
            return out
        return df


def map_and_normalize_columns_for_tas(df: pd.DataFrame) -> pd.DataFrame:
        """Map common alternative column names to the names expected by TAS logic.

        Expected output columns used by `compute_tas_for_dataframe`:
        - `time` (integer seconds since epoch)
        - `latitude`, `longitude` (floats)
        - `altitude` (ft)
        - `ground_speed` (knots or units already present)
        - `track` (degrees)
        """
        out = df.copy()

        # Copy lat/lon if present under other canonical names
        if "lat" in out.columns and "latitude" not in out.columns:
            out["latitude"] = pd.to_numeric(out["lat"], errors="coerce")
        if "lon" in out.columns and "longitude" not in out.columns:
            out["longitude"] = pd.to_numeric(out["lon"], errors="coerce")

        # altitude
        for alt_name in ["altitude", "alt", "height", "alt_ft", "altitude_ft"]:
            if alt_name in out.columns and "altitude" not in out.columns:
                out["altitude"] = pd.to_numeric(out[alt_name], errors="coerce")
                break

        # ground_speed
        for gs_name in ["ground_speed", "gs", "groundSpeed", "speed", "spd"]:
            if gs_name in out.columns and "ground_speed" not in out.columns:
                out["ground_speed"] = pd.to_numeric(out[gs_name], errors="coerce")
                break

        # track
        for trk_name in ["track", "trk", "heading", "course", "direction"]:
            if trk_name in out.columns and "track" not in out.columns:
                out["track"] = pd.to_numeric(out[trk_name], errors="coerce")
                break

        # time: accept numeric 'time' or datetime-like 'timestamp'
        if "time" not in out.columns:
            if "timestamp" in out.columns:
                try:
                    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
                    out["time"] = ts.view(np.int64) // 1_000_000_000
                except Exception:
                    # fallback: numeric conversion
                    out["time"] = pd.to_numeric(out["timestamp"], errors="coerce")
        else:
            out["time"] = pd.to_numeric(out["time"], errors="coerce")

        # Normalize time to seconds if millisecond timestamps are detected
        if "time" in out.columns:
            med = out["time"].median(skipna=True)
            if pd.notna(med) and med > 1e12:
                out["time"] = (out["time"] / 1000.0).astype("Int64")
            else:
                out["time"] = out["time"].astype("Int64")

        # Convert negative longitude to [0,360) if present
        if "longitude" in out.columns:
            out.loc[out["longitude"] < 0, "longitude"] = (
                out.loc[out["longitude"] < 0, "longitude"] % 360.0
            )

        return out


def prepare_adsb_for_tas(df: pd.DataFrame) -> pd.DataFrame:
        """High-level preparer that returns a DataFrame ready for TAS calculation.

        Raises ValueError if essential columns are missing after normalization.
        """
        df2 = parse_position_column(df, pos_col="Position")
        df2 = map_and_normalize_columns_for_tas(df2)

        required = ["time", "latitude", "longitude", "ground_speed", "track"]
        missing = [c for c in required if c not in df2.columns]
        if missing:
            raise ValueError(f"Missing required columns for TAS: {', '.join(missing)}")

        # Drop rows without required values
        df2 = df2.dropna(subset=required)
        return df2



# ============================================================================
# Temperature helpers (ERA5 + barometric lapse rate)
# ============================================================================


def _find_temperature_variable(ds):
    """Return the first plausible temperature variable name in an ERA5 dataset."""
    preferred = ["t2m", "t", "temperature"]
    for name in preferred:
        if name in ds.variables:
            return name
    return next((v for v in ds.variables if "temp" in v.lower()), None)


def sample_temperature_era5(ds, lat: float, lon: float, t_unix: float) -> float:
    """
    ดึงอุณหภูมิ (K) ที่ระดับน้ำทะเลจาก ERA5 ตามตำแหน่ง/เวลา
    ใช้แค่แถวแรกของไฟล์ ADS-B (sea-level assumption)
    """
    t_dt = pd.to_datetime(t_unix, unit="s", utc=True)
    sel_time = ds.sel(time=t_dt, method="nearest")

    lon_era = ((lon + 180) % 360) - 180
    temp_var = _find_temperature_variable(ds)
    if temp_var is None:
        return np.nan

    try:
        # ถ้ามีมิติระดับความกด เลือก 0 ft -> ~1013 hPa
        if any(dim in ds[temp_var].dims for dim in ["level", "pressure_level", "isobaricInhPa"]):
            lev_n = next(
                (d for d in ds[temp_var].dims if d in ["level", "pressure_level", "isobaricInhPa"]),
                None,
            )
            p0 = pressure_hPa_from_alt_ft(0.0)
            point_t = sel_time[temp_var].sel(
                {lev_n: p0, "latitude": lat, "longitude": lon_era}, method="nearest"
            )
        else:
            lat_n = "latitude" if "latitude" in ds[temp_var].dims else "lat"
            lon_n = "longitude" if "longitude" in ds[temp_var].dims else "lon"
            point_t = sel_time[temp_var].sel({lat_n: lat, lon_n: lon_era}, method="nearest")

        return float(point_t.values)
    except Exception:
        return np.nan


def compute_temperature_profile_from_adsb_csv(
    file_obj: Union[BinaryIO, IOBase], era5_ds
) -> pd.DataFrame:
    """
    อ่านไฟล์ ADS-B (.csv) แล้วคำนวณอุณหภูมิแต่ละแถว

    - ใช้ ERA5 หา T0 ที่ระดับน้ำทะเลจาก "บรรทัดแรก" ของไฟล์
    - แถวถัดไปใช้อสมการ lapse rate: T = T0 - (L * h)
      L = 0.0065 K/m, h จาก altitude (ft -> m)
    """
    df = pd.read_csv(file_obj)
    df = _ensure_required_columns(df)

    if df.empty:
        return df.assign(temperature_K=np.nan)

    first_ts = df.loc[0, "timestamp"]
    t_unix = pd.Timestamp(first_ts).timestamp()
    t0 = sample_temperature_era5(
        era5_ds, float(df.loc[0, "lat"]), float(df.loc[0, "lon"]), t_unix
    )

    if np.isnan(t0):
        # ถ้าอ่าน ERA5 ไม่ได้ ให้คืนคอลัมน์ว่างเพื่อไม่หยุดการทำงาน
        return df.assign(temperature_K=np.nan)

    L = 0.0065  # K/m
    alt_m = df["altitude"].astype(float) * 0.3048
    df_out = df.copy()
    df_out["temperature_K"] = t0 - (L * alt_m)
    return df_out