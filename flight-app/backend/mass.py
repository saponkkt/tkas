from __future__ import annotations

import pandas as pd

# แยกวันที่และเวลา จากคอลัมน์ UTC column N

def add_utc_split_columns(df: pd.DataFrame, utc_col: str = "UTC") -> pd.DataFrame:
    """
    เพิ่มคอลัมน์วันที่และเวลา จากคอลัมน์ UTC ที่เป็น ISO 8601
    เช่น 2025-09-03T05:51:21Z -> UTC_date = 2025-09-03, UTC_time = 05:51:21
    """
    df_out = df.copy()
    dt = pd.to_datetime(df_out[utc_col], utc=True, errors="coerce")
    df_out[f"{utc_col}_date"] = dt.dt.date
    df_out[f"{utc_col}_time"] = dt.dt.strftime("%H:%M:%S")
    return df_out

