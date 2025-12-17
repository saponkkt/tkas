from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np


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


def get_config_param_series(df: pd.DataFrame, param: str, type_col: Optional[str] = None) -> pd.Series:
    """Return a Series of config[param] looked up per-row by resolved type key.

    Non-numeric values are returned as-is (object dtype). Use `pd.to_numeric` if
    numeric conversion is required by the caller.
    """
    config = _load_config()
    type_keys = get_type_key_series(df, type_col=type_col)

    return type_keys.map(lambda k: None if pd.isna(k) else config.get(k, {}).get(param))


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


def compute_thrust_N(df: pd.DataFrame, type_col: Optional[str] = None, phase_col: str = "flight_phase", alt_col: str = "altitude", temp_col: str = "temperature_K", ref: float = 288.15) -> pd.Series:
    """Compute Thrust_N per-row according to flight phase and config coefficients.

    Rules (per your specification):
    - Taxi_out, Landing, Taxi_in: Thr_des_ld = CTdes_ld * Thr_max_climb
    - Takeoff, Initial_climb, Climb: use Thr_max_climb
    - Cruise: Thr_cruise_max = CTcr * Thr_max_climb
    - Descent: if altitude >= Hp_des -> CTdes_high * Thr_max_climb else CTdes_low * Thr_max_climb
    - Approach: Thr_des_app = CTdes_app * Thr_max_climb

    Coefficients loaded from `config.json`: `CTcr`, `CTdes_high`, `CTdes_low`, `CTdes_app`, `CTdes_ld`, `Hp_des`.
    Returns a numeric Series aligned with `df.index` (NaN where inputs missing).
    """
    # ensure phase column exists
    phases = df.get(phase_col)
    if phases is None:
        # try detecting phases if not present
        try:
            from .flight_phase import detect_flight_phase

            df_with_phase = detect_flight_phase(df)
            phases = df_with_phase["flight_phase"]
        except Exception:
            phases = pd.Series([pd.NA] * len(df), index=df.index)

    thr_max = compute_thr_max_climb(df, type_col=type_col, temp_col=temp_col, ref=ref)

    # load coefficients per-row (numeric where appropriate)
    CTcr = pd.to_numeric(get_config_param_series(df, "CTcr", type_col=type_col), errors="coerce")
    CTdes_high = pd.to_numeric(get_config_param_series(df, "CTdes_high", type_col=type_col), errors="coerce")
    CTdes_low = pd.to_numeric(get_config_param_series(df, "CTdes_low", type_col=type_col), errors="coerce")
    CTdes_app = pd.to_numeric(get_config_param_series(df, "CTdes_app", type_col=type_col), errors="coerce")
    CTdes_ld = pd.to_numeric(get_config_param_series(df, "CTdes_ld", type_col=type_col), errors="coerce")
    Hp_des = pd.to_numeric(get_config_param_series(df, "Hp_des", type_col=type_col), errors="coerce")

    alt = pd.to_numeric(df.get(alt_col), errors="coerce")

    # prepare output series filled with NaN
    thrust = pd.Series([np.nan] * len(df), index=df.index, dtype=float)

    # Phase groups
    taxi_phases = set(["Taxi_out", "Landing", "Taxi_in"])
    climb_phases = set(["Takeoff", "Initial_climb", "Climb"])
    cruise_phases = set(["Cruise"])
    descent_phases = set(["Descent"])
    approach_phases = set(["Approach"])

    # 1. Taxi_out, Landing, Taxi_in -> CTdes_ld * thr_max
    mask_taxi = phases.isin(taxi_phases)
    thrust.loc[mask_taxi] = (CTdes_ld * thr_max).loc[mask_taxi]

    # 2. Takeoff, Initial_climb, Climb -> thr_max
    mask_climb = phases.isin(climb_phases)
    thrust.loc[mask_climb] = thr_max.loc[mask_climb]

    # 3. Cruise -> CTcr * thr_max
    mask_cruise = phases.isin(cruise_phases)
    thrust.loc[mask_cruise] = (CTcr * thr_max).loc[mask_cruise]

    # 4. Descent -> depends on Hp > Hp_des or Hp <= Hp_des
    mask_descent = phases.isin(descent_phases)
    if mask_descent.any():
        # where Hp > Hp_des and both are numeric
        cond_high = mask_descent & (~alt.isna()) & (~Hp_des.isna()) & (alt > Hp_des)
        cond_low = mask_descent & (~alt.isna()) & (~Hp_des.isna()) & (alt <= Hp_des)
        # apply
        thrust.loc[cond_high] = (CTdes_high * thr_max).loc[cond_high]
        thrust.loc[cond_low] = (CTdes_low * thr_max).loc[cond_low]
        # if Hp_des is missing, fall back to CTdes_low
        cond_no_hp = mask_descent & (Hp_des.isna())
        thrust.loc[cond_no_hp] = (CTdes_low * thr_max).loc[cond_no_hp]

    # 5. Approach -> CTdes_app * thr_max
    mask_approach = phases.isin(approach_phases)
    thrust.loc[mask_approach] = (CTdes_app * thr_max).loc[mask_approach]

    return thrust
