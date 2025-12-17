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
