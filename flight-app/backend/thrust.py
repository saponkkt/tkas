from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd


def _load_config() -> dict:
    cfg_path = Path(__file__).resolve().parent / "config.json"
    try:
        with cfg_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _resolve_type_key(val: object, config: dict) -> Optional[str]:
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    if s in config:
        return s
    import re

    m = re.search(r"(320|737|\d{3})", s)
    if m:
        candidate = m.group(1)
        if candidate in config:
            return candidate
    for k in config.keys():
        if k in s or s in k:
            return k
    return None


def compute_thr_max_climb_ISA(df: pd.DataFrame, type_col: Optional[str] = None) -> pd.Series:
    """Compute Thr_max_climb_ISA per-row using CTc1,CTc2,CTc3 from config.json.

    Formula: CTc1*(1-(Hp/CTc2)+CTc3*(Hp**2))
    Hp is taken from `altitude` column (assumed in feet).
    Returns a pandas Series aligned with df index.
    """
    config = _load_config()

    # detect type_col if not provided
    if type_col is None:
        possible_type_cols = [
            "aircraft_type",
            "type",
            "model",
            "icaoType",
            "icao",
            "aircraft",
            "registration",
        ]
        for c in possible_type_cols:
            if c in df.columns:
                type_col = c
                break

    def _row_val(row: pd.Series) -> object:
        type_val = row.get(type_col) if type_col is not None else None
        type_key = _resolve_type_key(type_val, config) if type_val is not None else None
        if type_key is None or type_key not in config:
            return pd.NA
        try:
            CTc1 = config[type_key].get("CTc1")
            CTc2 = config[type_key].get("CTc2")
            CTc3 = config[type_key].get("CTc3")
            CTc1 = float(CTc1) if CTc1 is not None else None
            CTc2 = float(CTc2) if CTc2 is not None else None
            CTc3 = float(CTc3) if CTc3 is not None else None
            if CTc1 is None or CTc2 is None or CTc3 is None:
                return pd.NA
            Hp = row.get("altitude")
            Hp = float(Hp) if not pd.isna(Hp) else None
            if Hp is None:
                return pd.NA
            val = CTc1 * (1.0 - (Hp / CTc2) + CTc3 * (Hp ** 2))
            return float(val)
        except Exception:
            return pd.NA

    return df.apply(_row_val, axis=1)
