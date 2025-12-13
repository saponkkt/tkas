from __future__ import annotations

import math
import numpy as np
import pandas as pd

# แยกเวลา จากคอลัมน์ UTC column N

def add_utc_split_columns(df: pd.DataFrame, utc_col: str = "UTC") -> pd.DataFrame:
    """
    เพิ่มคอลัมน์เวลา จากคอลัมน์ UTC ที่เป็น ISO 8601
    เช่น 2025-09-03T05:51:21Z -> UTC_time = 05:51:21
    """
    df_out = df.copy()
    dt = pd.to_datetime(df_out[utc_col], utc=True, errors="coerce")
    df_out["time"] = dt.dt.strftime("%H:%M:%S")
    
    # ผลต่างเวลาระหว่างแถว (วินาที) แถวแรกเป็น 0
    delta_seconds = dt.diff().dt.total_seconds().fillna(0).astype(int)
    df_out["delta_t (s)"] = delta_seconds

    # ผลรวมสะสมของผลต่างเวลา (วินาที) แถวแรกเริ่มที่ 0
    df_out["sum_t (s)"] = delta_seconds.cumsum().astype(int)

    # แปลงผลรวมสะสมเป็นหน่วยนาที (ทศนิยมได้) ทุกแถว
    df_out["sum_t (min)"] = df_out["sum_t (s)"] / 60

    # ความดันบรรยากาศ (Pa) ด้วยสูตรบาโรเมตริก ใช้ T0 คงที่ (ISA)
    P0 = 101325  # Pa
    L = 0.0065  # K/m
    M = 0.0289652  # kg/mol
    g = 9.80665  # m/s^2
    R = 8.31446  # J/(mol·K)
    t0 = 288.15 # K

    alt_col = "altitude"
    alt_m = pd.to_numeric(df_out.get(alt_col), errors="coerce") * 0.3048

    exponent = (g * M) / (R * L)
    df_out["Pressure_Pa"] = P0 * (1 - (L * alt_m) / t0) ** exponent
    
    # แปลง TAS จาก knots เป็น m/s (คูณด้วย 0.5144)
    # ตรวจสอบทั้งคอลัมน์ TAS และ TAS_kt
    if "TAS_kt" in df_out.columns:
        df_out["TAS_m/s"] = pd.to_numeric(df_out["TAS_kt"], errors="coerce") * 0.5144
    else:
        # ถ้าไม่มีคอลัมน์ TAS ให้สร้างคอลัมน์ว่าง
        df_out["TAS_m/s"] = pd.NA
    
    return df_out

