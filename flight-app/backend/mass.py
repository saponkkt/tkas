from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Optional

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
    
    # คำนวณความเร่ง a_m/s^2 = ΔTAS_m/s / Δt
    # ผลต่างของ TAS_m/s ระหว่างแถวถัดไปกับแถวก่อนหน้า
    delta_tas = df_out["TAS_m/s"].diff()
    # หารด้วย delta_t (s) แต่ต้องระวังการหารด้วยศูนย์
    delta_t = df_out["delta_t (s)"].replace(0, pd.NA)  # แทนที่ 0 ด้วย NA เพื่อหลีกเลี่ยงการหารด้วยศูนย์
    df_out["a_m/s^2"] = delta_tas / delta_t
    
    # คำนวณความหนาแน่นอากาศ Density = Pressure_Pa / (R * temperature_K)
    # R = 287.05 J/(kg·K) (gas constant สำหรับอากาศ)
    R_air = 287.05  # J/(kg·K)
    if "temperature_K" in df_out.columns:
        temperature_K = pd.to_numeric(df_out["temperature_K"], errors="coerce")
        df_out["Density"] = df_out["Pressure_Pa"] / (R_air * temperature_K)
    else:
        # ถ้าไม่มีคอลัมน์ temperature_K ให้สร้างคอลัมน์ว่าง
        df_out["Density"] = pd.NA

    # เพิ่มคอลัมน์ S_m^2: เลือก wing area ตาม flight_phase และชนิดเครื่องบิน
    # พารามิเตอร์เสริมที่รองรับ:
    # - ถ้า DataFrame มีคอลัมน์ 'flight_phase' จะใช้ค่านั้น
    # - ชื่อชนิดเครื่องบินจะพยายามหาในคอลัมน์ที่เป็นไปได้ (เช่น 'type','aircraft_type','model')
    # - ถ้าต้องการบังคับชนิดเครื่องบิน ระบุเป็นสตริงในคีย์ config (เช่น '320' หรือ '737')
    def _load_config() -> dict:
        cfg_path = Path(__file__).resolve().parent / "config.json"
        try:
            with cfg_path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}

    config = _load_config()

    # helper to resolve type key from a free-form value
    def _resolve_type_key(val: object) -> Optional[str]:
        if pd.isna(val):
            return None
        s = str(val).strip()
        if not s:
            return None
        # direct match
        if s in config:
            return s
        # digits match (e.g., 'A320' -> '320')
        m = re.search(r"(320|737|\d{3})", s)
        if m:
            candidate = m.group(1)
            if candidate in config:
                return candidate
        # fallback: check if any config key is substring of s or vice-versa
        for k in config.keys():
            if k in s or s in k:
                return k
        return None

    # flight_phase column
    if "flight_phase" not in df_out.columns:
        # ถ้าไม่มี flight_phase ให้ไม่ใส่ค่าใน S_m^2 (หรืออาจเรียก detect_flight_phase ก่อน)
        df_out["S_m^2"] = pd.NA
        return df_out

    # determine aircraft type column (pick first existing)
    possible_type_cols = [
        "aircraft_type",
        "type",
        "model",
        "icaoType",
        "icao",
        "aircraft",
        "registration",
    ]
    type_col = None
    for c in possible_type_cols:
        if c in df_out.columns:
            type_col = c
            break

    # vectorized selection per-row
    def _select_area(row: pd.Series) -> object:
        phase = row.get("flight_phase")
        phase_l = str(phase).lower() if not pd.isna(phase) else ""

        # determine type key
        type_val = None
        if type_col is not None:
            type_val = row.get(type_col)
        # resolve to config key
        type_key = _resolve_type_key(type_val) if type_val is not None else None

        # default areas
        wing_clean = None
        wing_flap = None
        if type_key is not None and type_key in config:
            wing_clean = config[type_key].get("wing_area_clean")
            wing_flap = config[type_key].get("wing_area_flap")

        # mapping rules (phase -> which area to use)
        # phases treated case-insensitively
        flap_phases = {"takeoff", "initial_climb", "initialclimb", "initial-climb", "landing"}
        # normalize some common variants
        normalized = phase_l.replace(" ", "_")

        use_flap = False
        if normalized in flap_phases:
            use_flap = True
        else:
            # explicit mapping for names used in flight_phase.py
            if normalized == "taxi_out":
                use_flap = False
            elif normalized == "takeoff":
                use_flap = True
            elif normalized == "initial_climb":
                use_flap = True
            elif normalized == "climb":
                use_flap = False
            elif normalized == "cruise":
                use_flap = False
            elif normalized == "descent":
                use_flap = False
            elif normalized == "approach":
                use_flap = False
            elif normalized == "landing":
                use_flap = True
            elif normalized == "taxi_in":
                use_flap = False
            else:
                # unknown phase -> prefer clean
                use_flap = False

        area = pd.NA
        if use_flap:
            area = wing_flap if wing_flap is not None else pd.NA
        else:
            area = wing_clean if wing_clean is not None else pd.NA

        # ensure numeric
        try:
            if area is pd.NA:
                return pd.NA
            return float(area)
        except Exception:
            return pd.NA

    df_out["S_m^2"] = df_out.apply(_select_area, axis=1)
    
    return df_out
