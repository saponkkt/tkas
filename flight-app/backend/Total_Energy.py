"""Total_Energy helper

Provides utilities to compute and add `CL` and `CD` columns to a pandas DataFrame.

Formulas used:
    CL = (2 * mt_first * 9.80665) / (Density * (TAS_m/s)**2 * (S_m^2))
    CD = CD0 + CD0,deltaLDG + CD2 * (CL)**2

Assumes the DataFrame has columns named `mt`, `Density`, `TAS_m/s`, `S_m^2`,
`CD0`, `CD0,deltaLDG`, and `CD2` by default. You can pass alternative column
names to `add_CL` and `add_CD`.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def add_CL(
    df: pd.DataFrame,
    mt_col: str = "mt",
    density_col: str = "Density",
    tas_col: str = "TAS_m/s",
    s_col: str = "S_m^2",
    out_col: str = "CL",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add a `CL` column to `df` using the provided formula.

    The formula uses the first value of the `mt_col` column (row 0).
    """
    if not inplace:
        df = df.copy()

    for col in (mt_col, density_col, tas_col, s_col):
        if col not in df.columns:
            raise KeyError(f"required column '{col}' not found in DataFrame")

    try:
        mt_first = float(df[mt_col].iloc[0])
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"could not read first value of '{mt_col}': {exc}") from exc

    denom = df[density_col].astype(float) * (df[tas_col].astype(float) ** 2) * (
        df[s_col].astype(float) ** 2
    )

    with np.errstate(divide="ignore", invalid="ignore"):
        df[out_col] = (2.0 * mt_first * 9.80665) / denom

    return df


def add_CD(
    df: pd.DataFrame,
    cd0_col: str = "CD0",
    cd0_delta_col: str = "CD0,deltaLDG",
    cd2_col: str = "CD2",
    cl_col: str = "CL",
    out_col: str = "CD",
    inplace: bool = False,
) -> pd.DataFrame:
    """Add a `CD` column to `df` using the provided formula.

    CD = CD0 + CD0,deltaLDG + CD2 * (CL)**2
    """
    if not inplace:
        df = df.copy()

    for col in (cd0_col, cd0_delta_col, cd2_col):
        if col not in df.columns:
            raise KeyError(f"required column '{col}' not found in DataFrame")

    if cl_col not in df.columns:
        raise KeyError(f"required column '{cl_col}' not found in DataFrame; compute CL first")

    df[out_col] = (
        df[cd0_col].astype(float)
        + df[cd0_delta_col].astype(float)
        + df[cd2_col].astype(float) * (df[cl_col].astype(float) ** 2)
    )

    return df


