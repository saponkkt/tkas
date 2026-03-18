import os
import math
import time
import warnings
from typing import Tuple

import pandas as pd
import numpy as np
import xarray as xr
import traceback

# ปิด SerializationWarning จาก xarray
warnings.filterwarnings("ignore", category=xr.SerializationWarning)

def _normalize_input_df(df: pd.DataFrame) -> pd.DataFrame:
    # case-insensitive normalization: map common variants to canonical names
    col_map = {}
    lower_to_actual = {c.lower(): c for c in df.columns}

    def pick(*choices):
        for ch in choices:
            if ch.lower() in lower_to_actual:
                return lower_to_actual[ch.lower()]
        return None

    # time / timestamp
    tcol = pick('time', 'timestamp', 'ts')
    if tcol:
        col_map[tcol] = 'time'

    # utc
    ucol = pick('utc', 'utc_time', 'UTC')
    if ucol:
        col_map[ucol] = 'utc_time'

    # altitude variants
    acol = pick('altitude', 'alt', 'elevation', 'height', 'alt_ft', 'alt_m')
    if acol:
        col_map[acol] = 'altitude'

    # speed
    scol = pick('ground_speed', 'speed', 'gs', 'TAS_kt')
    if scol:
        col_map[scol] = 'ground_speed'

    # track / heading / direction
    trcol = pick('track', 'direction', 'heading')
    if trcol:
        col_map[trcol] = 'track'

    # lat/lon
    latc = pick('latitude', 'lat')
    lonc = pick('longitude', 'lon', 'long')
    if latc:
        col_map[latc] = 'latitude'
    if lonc:
        col_map[lonc] = 'longitude'

    # position-like
    posc = pick('position', 'pos', 'latlon', 'lat_long')
    if posc:
        col_map[posc] = 'Position'

    # apply renames
    if col_map:
        df = df.rename(columns=col_map)

    # parse Position into lat/lon if present
    if 'Position' in df.columns and ('latitude' not in df.columns or 'longitude' not in df.columns):
        split_cols = None
        for sep in [',', ' ', ';', '|']:
            try:
                split_cols = df['Position'].astype(str).str.split(sep, expand=True)
                if split_cols.shape[1] >= 2:
                    break
            except Exception:
                pass
        if split_cols is not None and split_cols.shape[1] >= 2:
            df['latitude'] = pd.to_numeric(split_cols[0], errors='coerce')
            df['longitude'] = pd.to_numeric(split_cols[1], errors='coerce')

    # ensure required cols exist (create with NaN defaults)
    required = ['time', 'latitude', 'longitude', 'altitude', 'ground_speed', 'track']
    for c in required:
        if c not in df.columns:
            df[c] = np.nan

    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", "ground_speed", "track"]) 
    if len(df) == 0:
        return df

    # ms -> s
    if df["time"].median() > 1e12:
        df["time"] = (df["time"] / 1000.0).astype(np.int64)
    else:
        df["time"] = df["time"].astype(np.int64)

    # convert longitude for GFS (0..360)
    df.loc[df["longitude"] < 0, "longitude"] = df["longitude"] % 360.0

    return df