from __future__ import annotations

import numpy as np
import pandas as pd


def add_P1_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P1` column computed.

    Formula:
    P1 = 2 * K * (9.80665**2) * (cos(gamma_rad)**2) /
         (Density * (TAS_m/s)**2 * S_m^2)

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities with NA.
    """
    df_out = df.copy()
    try:
        g = 9.80665
        K = pd.to_numeric(df_out.get("K"), errors="coerce")
        density = pd.to_numeric(df_out.get("Density"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")
        S = pd.to_numeric(df_out.get("S_m^2"), errors="coerce")
        gamma = pd.to_numeric(df_out.get("gamma_rad"), errors="coerce")

        cos2 = np.cos(gamma) ** 2
        denom = density * (tas ** 2) * S

        df_out["P1"] = (2 * K * (g ** 2) * cos2) / denom
        df_out["P1"] = df_out["P1"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P1"] = pd.NA

    return df_out

def add_P2_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P2` column computed.

    Formula:
    P2 = a_m/s^2 + 9.80665 * (ROCD_m/s) / (TAS_m/s)

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities or invalid values with NA.
    """
    df_out = df.copy()
    try:
        g = 9.80665
        a = pd.to_numeric(df_out.get("a_m/s^2"), errors="coerce")
        rocd = pd.to_numeric(df_out.get("ROCD_m/s"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")

        term = rocd / tas
        df_out["P2"] = a + (g * term)
        df_out["P2"] = df_out["P2"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P2"] = pd.NA

    return df_out

def add_P3_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with a new `P3` column computed.

    Formula:
    P3 = CD0 * 0.5 * Density * (TAS_m/s)^2 * S_m^2 - Thrust_N * 0.76

    The function is defensive: it converts inputs to numeric where possible
    and replaces infinities with NA.
    """
    df_out = df.copy()
    try:
        cd0 = pd.to_numeric(df_out.get("CD0"), errors="coerce")
        density = pd.to_numeric(df_out.get("Density"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")
        S = pd.to_numeric(df_out.get("S_m^2"), errors="coerce")
        thrust = pd.to_numeric(df_out.get("Thrust_N"), errors="coerce")

        aero_term = 0.5 * density * (tas ** 2) * S

        df_out["P3"] = (cd0 * aero_term) - (thrust * 0.76)
        df_out["P3"] = df_out["P3"].replace([np.inf, -np.inf], pd.NA)
    except Exception:
        df_out["P3"] = pd.NA

    return df_out




