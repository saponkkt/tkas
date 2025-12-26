'''from __future__ import annotations

import os
from io import IOBase
from typing import BinaryIO, Dict, Optional, Union

import math

import numpy as np
import pandas as pd
import xarray as xr

from haversine import haversine_nm
import time


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

