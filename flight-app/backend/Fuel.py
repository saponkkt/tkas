from __future__ import annotations

from typing import Optional

import pandas as pd

from thrust import get_config_param_series


def compute_eta_kg_per_min_per_kN(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.Series:
    """คำนวณคอลัมน์ eta_kg/min*kN ตามสูตร:

    eta = Cf1 * (1 + TAS_kt / Cf2)

    โดยที่ Cf1, Cf2 อ่านมาจาก `config.json` ตามชนิดเครื่องบิน (type_col).
    - หน่วย Cf1, Cf2 สมมติให้สอดคล้องกับสูตรที่กำหนด
    - คืนค่าเป็น Series ที่ index ตรงกับ df เดิม (ชื่อคอลัมน์: "eta_kg_per_min_per_kN")
    """
    # ดึง Cf1, Cf2 ต่อแถวจาก config ตามชนิดเครื่องบิน
    cf1 = pd.to_numeric(get_config_param_series(df, "Cf1", type_col=type_col), errors="coerce")
    cf2 = pd.to_numeric(get_config_param_series(df, "Cf2", type_col=type_col), errors="coerce")

    # ความเร็ว TAS จากคอลัมน์ใน DataFrame
    tas = pd.to_numeric(df.get(tas_col), errors="coerce")

    # คำนวณ eta โดยจัดการกรณี cf2 = 0 หรือ NaN ให้กลายเป็น NaN อัตโนมัติ
    with pd.option_context("mode.use_inf_as_na", True):
        eta = cf1 * (1.0 + (tas / cf2))

    eta.name = "eta_kg_per_min_per_kN"
    return eta


def add_eta_column(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `eta_kg_per_min_per_kN` เข้าไป.

    ใช้สูตรเดียวกับ `compute_eta_kg_per_min_per_kN`.
    """
    df_out = df.copy()
    df_out["eta_kg_per_min_per_kN"] = compute_eta_kg_per_min_per_kN(
        df_out, type_col=type_col, tas_col=tas_col
    )
    return df_out


def compute_fnom_kg_per_s(
    df: pd.DataFrame,
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
) -> pd.Series:
    """คำนวณคอลัมน์ fnom_kg/s ตามสูตร:

    fnom = eta_kg_per_min_per_kN * Thrust_N * 10^-3 / 60

    โดย:
    - `eta_kg_per_min_per_kN` มาจากคอลัมน์ eta ที่คำนวณจาก `compute_eta_kg_per_min_per_kN`
    - `Thrust_N` มาจากผลการคำนวณในไฟล์ thrust (คอลัมน์ชื่อเดียวกันใน DataFrame)
    """
    eta = pd.to_numeric(df.get(eta_col), errors="coerce")
    thrust = pd.to_numeric(df.get(thrust_col), errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        fnom = eta * thrust * 1e-3 / 60.0

    fnom.name = "fnom_kg_per_s"
    return fnom


def add_fnom_column(
    df: pd.DataFrame,
    thrust_col: str = "Thrust_N",
    eta_col: str = "eta_kg_per_min_per_kN",
    type_col: Optional[str] = "aircraft_type",
    tas_col: str = "TAS_kt",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fnom_kg_per_s`.

    - ถ้ายังไม่มีคอลัมน์ `eta_kg_per_min_per_kN` จะคำนวณเพิ่มให้อัตโนมัติ
    - ใช้สูตรเดียวกับ `compute_fnom_kg_per_s`.
    """
    df_out = df.copy()

    # ถ้ายังไม่มี eta ให้คำนวณเพิ่มก่อน
    if eta_col not in df_out.columns:
        df_out["eta_kg_per_min_per_kN"] = compute_eta_kg_per_min_per_kN(
            df_out, type_col=type_col, tas_col=tas_col
        )

    df_out["fnom_kg_per_s"] = compute_fnom_kg_per_s(
        df_out, thrust_col=thrust_col, eta_col=eta_col
    )
    return df_out


def compute_fmin_kg_per_s(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
) -> pd.Series:
    """คำนวณคอลัมน์ fmin_kg/s ตามสูตร:

    fmin = Cf3 * (1 - altitude / Cf4) / 60

    โดย:
    - `Cf3`, `Cf4` อ่านจาก `config.json` ตามชนิดเครื่องบิน (type_col)
    - `altitude` คือคอลัมน์ความสูง (หน่วย ft) ใน DataFrame
    """
    cf3 = pd.to_numeric(get_config_param_series(df, "Cf3", type_col=type_col), errors="coerce")
    cf4 = pd.to_numeric(get_config_param_series(df, "Cf4", type_col=type_col), errors="coerce")
    alt = pd.to_numeric(df.get(alt_col), errors="coerce")

    with pd.option_context("mode.use_inf_as_na", True):
        fmin = cf3 * (1.0 - (alt / cf4)) / 60.0

    fmin.name = "fmin_kg_per_s"
    return fmin


def add_fmin_column(
    df: pd.DataFrame,
    type_col: Optional[str] = "aircraft_type",
    alt_col: str = "altitude",
) -> pd.DataFrame:
    """คืน DataFrame ใหม่ที่เพิ่มคอลัมน์ `fmin_kg_per_s` เข้าไป.

    ใช้สูตรเดียวกับ `compute_fmin_kg_per_s`.
    """
    df_out = df.copy()
    df_out["fmin_kg_per_s"] = compute_fmin_kg_per_s(
        df_out, type_col=type_col, alt_col=alt_col
    )
    return df_out
