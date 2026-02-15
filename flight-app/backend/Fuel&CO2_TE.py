"""Fuel & CO2 helpers for Total Energy pipeline.

This file provides a simple helper to compute `fnom` from the
`eta_kg_per_min_per_kN` (from `Fuel.py`) and `Thrust_N_TE` (from
`Total_Energy.py`).

Function:
- `add_fnom_TE(df, eta_col='eta_kg_per_min_per_kN', thrust_col='Thrust_N_TE', out_col='fnom')`

The function multiplies the two columns elementwise and stores the
result in `out_col`.
"""
from __future__ import annotations

import pandas as pd
from thrust import get_config_param_series
import numpy as np


def add_fnom_TE(
    df: pd.DataFrame,
    eta_col: str = "eta_kg_per_min_per_kN",
    thrust_col: str = "Thrust_N_TE",
    out_col: str = "fnom",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add `fnom` column computed as eta * Thrust_N_TE.

    Parameters
    - df: input DataFrame
    - eta_col: name of eta column (default: `eta_kg_per_min_per_kN`)
    - thrust_col: name of thrust column (default: `Thrust_N_TE`)
    - out_col: output column name (default: `fnom`)
    - inplace: if True modify df in-place

    Notes
    - This does a straight multiplication without unit conversion.
    - If either input column is missing a `KeyError` is raised.
    """
    if not inplace:
        df = df.copy()

    for col in (eta_col, thrust_col):
        if col not in df.columns:
            raise KeyError(f"required column '{col}' not found in DataFrame")

    df[out_col] = df[eta_col].astype(float) * df[thrust_col].astype(float) * 1e-3 / 60.0
    return df


def add_fmin_TE(
    df: pd.DataFrame,
    source_col: str = "fmin_kg_per_s",
    out_col: str = "fmin",
    inplace: bool = False,
    type_col: str = "aircraft_type",
    alt_col: str = "altitude",
) -> pd.DataFrame:
    """Ensure a `fmin` column exists by copying or computing from `Fuel.py`.

    - If `source_col` is present in `df`, copy it to `out_col`.
    - Otherwise try to import `add_fmin_column` from `Fuel` and compute it,
      then copy the computed `source_col` to `out_col`.
    """
    if not inplace:
        df = df.copy()

    if source_col not in df.columns:
        try:
            from Fuel import add_fmin_column

            df = add_fmin_column(df, type_col=type_col, alt_col=alt_col)
        except Exception as e:
            raise KeyError(f"required column '{source_col}' not found and Fuel.add_fmin_column failed: {e}") from e

    df[out_col] = pd.to_numeric(df[source_col], errors="coerce")
    return df


def add_fap_ld(
    df: pd.DataFrame,
    fnom_col: str = "fnom",
    fmin_col: str = "fmin",
    out_col: str = "fap/ld",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add `fap/ld` column as the elementwise max of `fnom` and `fmin`.

    Parameters
    - df: input DataFrame
    - fnom_col: name of the fnom column in this file (default: `fnom`)
    - fmin_col: name of the fmin column in this file (default: `fmin`)
    - out_col: output column name (default: `fap/ld`)
    - inplace: if True modify df in-place
    """
    if not inplace:
        df = df.copy()

    for col in (fnom_col, fmin_col):
        if col not in df.columns:
            raise KeyError(f"required column '{col}' not found in DataFrame")

    fnom = pd.to_numeric(df[fnom_col], errors="coerce")
    fmin = pd.to_numeric(df[fmin_col], errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        fap = pd.concat([fnom, fmin], axis=1).max(axis=1)

    df[out_col] = fap
    return df


__all__ = ["add_fnom_TE", "add_fmin_TE", "add_fap_ld"]


def add_fcr_TE(
    df: pd.DataFrame,
    type_col: str = "aircraft_type",
    eta_col: str = "eta_kg_per_min_per_kN",
    thrust_col: str = "Thrust_N_TE",
    out_col: str = "fcr",
) -> pd.DataFrame:
    """Add `fcr` column: eta * Thrust_N_TE * Cfcr * 1e-3 / 60.

    - `Cfcr` is read from `config.json` per aircraft type using
      `thrust.get_config_param_series`.
    - Returns DataFrame with `out_col` added.
    """
    df = df.copy()

    for col in (eta_col, thrust_col):
        if col not in df.columns:
            raise KeyError(f"required column '{col}' not found in DataFrame")

    eta = pd.to_numeric(df[eta_col], errors="coerce")
    thrust = pd.to_numeric(df[thrust_col], errors="coerce")
    cfcr = pd.to_numeric(get_config_param_series(df, "Cfcr", type_col=type_col), errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        fcr = eta * thrust * cfcr * 1e-3 / 60.0

    df[out_col] = fcr
    return df


__all__.append("add_fcr_TE")


def add_Fuel_TE(
    df: pd.DataFrame,
    phase_col: str = "flight_phase",
    thrust_col: str = "Thrust_N_TE",
    fnom_col: str = "fnom",
    fmin_col: str = "fmin",
    fcr_col: str = "fcr",
    fapld_col: str = "fap/ld",
    out_col: str = "Fuel_TE",
) -> pd.DataFrame:
    """Compute `Fuel_TE` per-row according to flight phase and rules.

    Rules (applied per-row):
    - Taxi_out -> use `fmin`
    - Takeoff -> use `fnom`, but if `Thrust_N_TE` > 1_000_000 use `fmin`
    - Initial_climb -> use `fnom`
    - Climb -> use `fnom`
    - Cruise -> use `fcr`
    - Descent -> use `fmin`
    - Approach -> use `fap/ld`
    - Landing -> use `fap/ld`, but if `Thrust_N_TE` > 1_000_000 use `fmin`
    - Taxi_in -> use `fmin`

    The function will attempt to compute missing helper columns by calling
    local helpers (`add_fnom_TE`, `add_fmin_TE`, `add_fcr_TE`, `add_fap_ld`).
    """
    df = df.copy()

    # ensure phase column
    phases = df.get(phase_col)
    if phases is None:
        raise KeyError(f"required column '{phase_col}' not found in DataFrame")

    # Ensure helper columns exist, computing them if possible
    if fnom_col not in df.columns:
        try:
            df = add_fnom_TE(df)
        except Exception:
            pass
    if fmin_col not in df.columns:
        try:
            df = add_fmin_TE(df)
        except Exception:
            pass
    if fcr_col not in df.columns:
        try:
            df = add_fcr_TE(df)
        except Exception:
            pass
    if fapld_col not in df.columns:
        try:
            df = add_fap_ld(df)
        except Exception:
            pass

    # now retrieve numeric series (may contain NaN)
    fnom = pd.to_numeric(df.get(fnom_col), errors="coerce")
    fmin = pd.to_numeric(df.get(fmin_col), errors="coerce")
    fcr = pd.to_numeric(df.get(fcr_col), errors="coerce")
    fapld = pd.to_numeric(df.get(fapld_col), errors="coerce")
    thrust = pd.to_numeric(df.get(thrust_col), errors="coerce")

    # prepare output series (use numeric NaN to avoid pd.NA -> float() issues)
    fuel = pd.Series(np.nan, index=df.index, dtype=float)

    # Masks
    mask_taxi_out = phases == "Taxi_out"
    mask_takeoff = phases == "Takeoff"
    mask_initial = phases == "Initial_climb"
    mask_climb = phases == "Climb"
    mask_cruise = phases == "Cruise"
    mask_descent = phases == "Descent"
    mask_approach = phases == "Approach"
    mask_landing = phases == "Landing"
    mask_taxi_in = phases == "Taxi_in"

    # apply rules
    fuel.loc[mask_taxi_out] = fmin.loc[mask_taxi_out]

    # Takeoff: fnom unless thrust > 1e6 then fmin
    take_idx = mask_takeoff & (thrust.fillna(0) <= 1_000_000)
    fuel.loc[take_idx] = fnom.loc[take_idx]
    take_idx_h = mask_takeoff & (thrust.fillna(0) > 1_000_000)
    fuel.loc[take_idx_h] = fmin.loc[take_idx_h]

    fuel.loc[mask_initial] = fnom.loc[mask_initial]
    
    # Climb: use fnom, but if fnom is negative use fmin instead
    climb_idx_pos = mask_climb & (fnom >= 0)
    climb_idx_neg = mask_climb & (fnom < 0)
    fuel.loc[climb_idx_pos] = fnom.loc[climb_idx_pos]
    fuel.loc[climb_idx_neg] = fmin.loc[climb_idx_neg]
    
    fuel.loc[mask_cruise] = fcr.loc[mask_cruise]
    fuel.loc[mask_descent] = fmin.loc[mask_descent]
    fuel.loc[mask_approach] = fapld.loc[mask_approach]

    # Landing: fap/ld unless thrust > 1e6 then fmin
    land_idx = mask_landing & (thrust.fillna(0) <= 1_000_000)
    fuel.loc[land_idx] = fapld.loc[land_idx]
    land_idx_h = mask_landing & (thrust.fillna(0) > 1_000_000)
    fuel.loc[land_idx_h] = fmin.loc[land_idx_h]

    fuel.loc[mask_taxi_in] = fmin.loc[mask_taxi_in]

    df[out_col] = fuel
    return df


__all__.append("add_Fuel_TE")


def add_Fuel_at_time_TE(
    df: pd.DataFrame,
    fuel_col: str = "Fuel_TE",
    delta_t_col: str = "delta_t (s)",
    out_col: str = "Fuel_at_time_TE",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add `Fuel_at_time_TE` = Fuel_TE * delta_t (s) per-row.

    - If `fuel_col` is missing the function will attempt to compute it via
      `add_Fuel_TE`.
    - `delta_t_col` is expected to be present (from `variable_mass.add_utc_split_columns`).
    """
    if not inplace:
        df = df.copy()

    if fuel_col not in df.columns:
        try:
            df = add_Fuel_TE(df)
        except Exception:
            raise KeyError(f"required column '{fuel_col}' not found and add_Fuel_TE failed to compute it")

    if delta_t_col not in df.columns:
        raise KeyError(f"required column '{delta_t_col}' not found in DataFrame")

    fuel = pd.to_numeric(df[fuel_col], errors="coerce")
    dt = pd.to_numeric(df[delta_t_col], errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        df[out_col] = fuel * dt

    return df


__all__.append("add_Fuel_at_time_TE")


def add_Fuel_sum_with_time_TE(
    df: pd.DataFrame,
    fuel_at_time_col: str = "Fuel_at_time_TE",
    out_col: str = "Fuel_sum_with_time_TE",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add cumulative sum of `fuel_at_time_col` as `out_col`.

    The cumulative sum is computed so that the first row is 0, and each
    subsequent row equals the sum of all previous `fuel_at_time_col` values.
    """
    if not inplace:
        df = df.copy()

    if fuel_at_time_col not in df.columns:
        try:
            df = add_Fuel_at_time_TE(df)
        except Exception:
            raise KeyError(f"required column '{fuel_at_time_col}' not found and add_Fuel_at_time_TE failed to compute it")

    fat = pd.to_numeric(df[fuel_at_time_col], errors="coerce").fillna(0)
    # cumulative sum including current row so row n = sum(fuel_at_time[:n+1])
    csum_inclusive = fat.cumsum()

    df[out_col] = csum_inclusive
    return df


__all__.append("add_Fuel_sum_with_time_TE")


def add_CO2_at_time_TE(
    df: pd.DataFrame,
    fuel_at_time_col: str = "Fuel_at_time_TE",
    out_col: str = "CO2_at_time",
    factor: float = 3.16,
    inplace: bool = False,
) -> pd.DataFrame:
    """Add `CO2_at_time` = `Fuel_at_time_TE` * factor per-row.

    - If `fuel_at_time_col` is missing the function will attempt to compute it
      via `add_Fuel_at_time_TE`.
    - `factor` defaults to 3.16 (kg CO2 per kg fuel).
    """
    if not inplace:
        df = df.copy()

    if fuel_at_time_col not in df.columns:
        try:
            df = add_Fuel_at_time_TE(df)
        except Exception:
            raise KeyError(f"required column '{fuel_at_time_col}' not found and add_Fuel_at_time_TE failed to compute it")

    fat = pd.to_numeric(df[fuel_at_time_col], errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        df[out_col] = fat * float(factor)

    return df


__all__.append("add_CO2_at_time_TE")

def add_CO2_sum_with_time_TE(
    df: pd.DataFrame,
    co2_at_time_col: str = "CO2_at_time",
    out_col: str = "CO2_sum_with_time",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add cumulative sum of `co2_at_time_col` as `out_col`.

    The cumulative sum is computed so that the first row is 0, and each
    subsequent row equals the sum of all previous `co2_at_time_col` values.
    """
    if not inplace:
        df = df.copy()

    if co2_at_time_col not in df.columns:
        try:
            df = add_CO2_at_time_TE(df)
        except Exception:
            raise KeyError(f"required column '{co2_at_time_col}' not found and add_CO2_at_time_TE failed to compute it")

    cat = pd.to_numeric(df[co2_at_time_col], errors="coerce").fillna(0)
    # inclusive cumulative sum: row n = sum(co2_at_time[:n+1])
    csum_inclusive = cat.cumsum()

    df[out_col] = csum_inclusive
    return df


__all__.append("add_CO2_sum_with_time_TE")