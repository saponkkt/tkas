from __future__ import annotations

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
    return df_out

