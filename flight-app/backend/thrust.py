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


def get_type_key_series(df: pd.DataFrame, type_col: Optional[str] = None) -> pd.Series:
    """Return a Series of resolved config type keys for each row in `df`.

    Resolves using `_resolve_type_key`. If `type_col` is not provided the
    function will attempt to detect a likely column name.
    """
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

    config = _load_config()
    if type_col is None or type_col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)

    return df[type_col].map(lambda v: _resolve_type_key(v, config))


def get_ctc_series(df: pd.DataFrame, type_col: Optional[str] = None) -> pd.DataFrame:
    """Return a DataFrame with per-row numeric CTc1,CTc2,CTc3,CTc4 values.

    Columns are named `CTc1`, `CTc2`, `CTc3`, `CTc4`. Missing values are
    represented as pandas `NaN` (float).
    """
    config = _load_config()
    type_keys = get_type_key_series(df, type_col=type_col)

    def _map_param(param: str) -> pd.Series:
        return type_keys.map(lambda k: None if pd.isna(k) else config.get(k, {}).get(param))

    s1 = pd.to_numeric(_map_param("CTc1"), errors="coerce")
    s2 = pd.to_numeric(_map_param("CTc2"), errors="coerce")
    s3 = pd.to_numeric(_map_param("CTc3"), errors="coerce")
    s4 = pd.to_numeric(_map_param("CTc4"), errors="coerce")
    s5 = pd.to_numeric(_map_param("CTc5"), errors="coerce")

    return pd.DataFrame({"CTc1": s1, "CTc2": s2, "CTc3": s3, "CTc4": s4, "CTc5": s5}, index=df.index)


def compute_thr_max_climb_ISA(df: pd.DataFrame, type_col: Optional[str] = None) -> pd.Series:
    """Compute Thr_max_climb_ISA per-row using CTc1,CTc2,CTc3 from config.json.

    Formula: CTc1*(1-(Hp/CTc2)+CTc3*(Hp**2))
    Hp is taken from `altitude` column (assumed in feet).
    Returns a pandas Series aligned with df index.
    """
    # build per-row CTc series and altitude
    ctc = get_ctc_series(df, type_col=type_col)
    Hp = pd.to_numeric(df.get("altitude"), errors="coerce")

    # formula: CTc1*(1-(Hp/CTc2)+CTc3*(Hp**2))
    with pd.option_context("mode.use_inf_as_na", True):
        val = ctc["CTc1"] * (1.0 - (Hp / ctc["CTc2"]) + ctc["CTc3"] * (Hp ** 2))

    # ensure index alignment and return numeric series (NaN where incomplete)
    return pd.Series(val, index=df.index)


def compute_delta_temp(df: pd.DataFrame, temp_col: str = "temperature_K", ref: float = 288.15) -> pd.Series:
    """Compute delta_temp = temperature_K - ref for each row and return a Series.

    Returns pd.NA for missing/non-numeric temperature values.
    """
    try:
        temps = pd.to_numeric(df.get(temp_col), errors="coerce")
        return temps - float(ref)
    except Exception:
        return pd.Series([pd.NA] * len(df), index=df.index)


def compute_delta_temp_eff(df: pd.DataFrame, type_col: Optional[str] = None, temp_col: str = "temperature_K", ref: float = 288.15) -> pd.Series:
    """Compute delta_temp_eff = delta_temp - CTc4 per-row.

    - delta_temp = temperature_K - ref
    - CTc4 is read from config.json for the resolved aircraft type
    Returns a pandas Series aligned with df index (pd.NA where unavailable).
    """
    # compute delta_temp and per-row CTc4, then subtract
    delta_temp = compute_delta_temp(df, temp_col=temp_col, ref=ref)
    ctc = get_ctc_series(df, type_col=type_col)
    ct4 = ctc["CTc4"]

    return delta_temp - ct4


def compute_thr_max_climb(df: pd.DataFrame, type_col: Optional[str] = None, temp_col: str = "temperature_K", ref: float = 288.15) -> pd.Series:
    """Compute Thr_max_climb per-row.

    Formula: Thr_max_climb = Thr_max_climb_ISA * (1 - CTc5 * delta_temp_eff)

    - `Thr_max_climb_ISA` is computed by `compute_thr_max_climb_ISA`.
    - `delta_temp_eff` is computed by `compute_delta_temp_eff`.
    - `CTc5` is read from `config.json` per resolved aircraft type.

    Returns a pandas Series aligned with `df.index`. Missing inputs yield NaN.
    """
    thr_isa = compute_thr_max_climb_ISA(df, type_col=type_col)
    delta_eff = compute_delta_temp_eff(df, type_col=type_col, temp_col=temp_col, ref=ref)
    ctc = get_ctc_series(df, type_col=type_col)

    # CTc5 may be missing; ensure alignment
    ct5 = ctc.get("CTc5") if "CTc5" in ctc.columns else pd.Series([pd.NA] * len(df), index=df.index)

    return thr_isa * (1.0 - ct5 * delta_eff)
