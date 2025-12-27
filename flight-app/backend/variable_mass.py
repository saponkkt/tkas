from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import sys

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
    # แทนค่าอนันต์ด้วย NA และตั้งแถวแรกเป็น 0.0 (ไม่มีแถวก่อนหน้า)
    df_out["a_m/s^2"] = df_out["a_m/s^2"].replace([np.inf, -np.inf], pd.NA)
    if len(df_out) > 0:
        try:
            df_out.at[df_out.index[0], "a_m/s^2"] = 0.0
        except Exception:
            pass

    # คำนวณความหนาแน่นอากาศ Density = Pressure_Pa / (R * temperature_K)
    # R = 287.05 J/(kg·K) (gas constant สำหรับอากาศ)
    R_air = 287.05  # J/(kg·K)
    if "Temperature_K" in df_out.columns:
        temperature_K = pd.to_numeric(df_out["Temperature_K"], errors="coerce")
        df_out["Density"] = df_out["Pressure_Pa"] / (R_air * temperature_K)
    else:
        # ถ้าไม่มีคอลัมน์ Temperature_K ให้สร้างคอลัมน์ว่าง
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
    
    # เพิ่มคอลัมน์ CD0: เลือกค่า CD0 จาก section ที่เหมาะสมใน config.json ตาม flight_phase
    phase_to_section = {
        "taxi_out": "CR",
        "takeoff": "TO",
        "initial_climb": "IC",
        "climb": "CR",
        "cruise": "CR",
        "descent": "CR",
        "approach": "AP",
        "landing": "LD",
        "taxi_in": "CR",
    }

    def _select_cd0(row: pd.Series) -> object:
        phase = row.get("flight_phase")
        phase_l = str(phase).lower() if not pd.isna(phase) else ""
        normalized = phase_l.replace(" ", "_")

        # determine type key
        type_val = None
        if type_col is not None:
            type_val = row.get(type_col)
        type_key = _resolve_type_key(type_val) if type_val is not None else None

        if type_key is None or type_key not in config:
            return pd.NA

        section = phase_to_section.get(normalized, "CR")
        try:
            sec_obj = config[type_key].get(section, {})
            cd0 = sec_obj.get("CD0") if isinstance(sec_obj, dict) else None
            if cd0 is None:
                # fallback: some configs might have CD0 at top level
                cd0 = config[type_key].get("CD0")
            if cd0 is None:
                return pd.NA
            return float(cd0)
        except Exception:
            return pd.NA

    df_out["CD0"] = df_out.apply(_select_cd0, axis=1)

    # เพิ่มคอลัมน์ CD2: เลือกค่า CD2 จาก section ที่เหมาะสมใน config.json ตาม flight_phase
    def _select_cd2(row: pd.Series) -> object:
        phase = row.get("flight_phase")
        phase_l = str(phase).lower() if not pd.isna(phase) else ""
        normalized = phase_l.replace(" ", "_")

        # determine type key
        type_val = None
        if type_col is not None:
            type_val = row.get(type_col)
        type_key = _resolve_type_key(type_val) if type_val is not None else None

        if type_key is None or type_key not in config:
            return pd.NA

        section = phase_to_section.get(normalized, "CR")
        try:
            sec_obj = config[type_key].get(section, {})
            cd2 = sec_obj.get("CD2") if isinstance(sec_obj, dict) else None
            if cd2 is None:
                # fallback: some configs might have CD2 at top level
                cd2 = config[type_key].get("CD2")
            if cd2 is None:
                return pd.NA
            return float(cd2)
        except Exception:
            return pd.NA

    df_out["CD2"] = df_out.apply(_select_cd2, axis=1)
    
    # เพิ่มคอลัมน์ CD0,deltaLDG: เลือกค่า CD0,deltaLDG จาก section ที่เหมาะสมใน config.json ตาม flight_phase
    # Compute integer positions for robust range checks (0..N-1)
    df_out["_pos"]= np.arange(len(df_out))
    alt_numeric = pd.to_numeric(df_out.get("altitude"), errors="coerce").fillna(-9999)
    phases_arr = df_out.get("flight_phase").astype(str).fillna("").to_numpy()

    # last position of Takeoff (if any)
    takeoff_pos = np.where(phases_arr == "Takeoff")[0]
    last_takeoff_pos = int(takeoff_pos.max()) if takeoff_pos.size > 0 else None

    # last position of Landing (if any)
    landing_pos = np.where(phases_arr == "Landing")[0]
    last_landing_pos = int(landing_pos.max()) if landing_pos.size > 0 else None

    # first position where altitude <= 1500 ft AND phase == 'Landing' (if any)
    landing_positions = np.where(phases_arr == "Landing")[0]
    first_alt_le1500_pos = None
    if landing_positions.size > 0:
        # consider only landing positions when searching for the 1500-ft threshold
        landing_alts = alt_numeric[landing_positions]
        idxs = np.where(landing_alts <= 1500)[0]
        if idxs.size > 0:
            # map back to global positions
            first_alt_le1500_pos = int(landing_positions[idxs.min()])

    # last position of Taxi_in (if any)
    taxi_in_pos = np.where(phases_arr == "Taxi_in")[0]
    last_taxi_in_pos = int(taxi_in_pos.max()) if taxi_in_pos.size > 0 else None

    def _select_cd0_deltaLDG(row: pd.Series) -> object:
        # Only assign CD0,deltaLDG for two ranges:
        # 1) from the first row up to last Takeoff row (inclusive)
        # 2) from the first row with altitude >=1500 ft up to last Landing row (inclusive)
        pos = int(row.get("_pos", -1))

        in_range = False
        if last_takeoff_pos is not None and pos <= last_takeoff_pos:
            in_range = True
        # second range: from first row in Landing where altitude <=1500 ft
        # up to the last Taxi_in row (if present). If Taxi_in not present,
        # fall back to last Landing position.
        second_range_end = last_taxi_in_pos if last_taxi_in_pos is not None else last_landing_pos
        if first_alt_le1500_pos is not None and second_range_end is not None and pos >= first_alt_le1500_pos and pos <= second_range_end:
            in_range = True

        if not in_range:
            # outside allowed ranges -> return 0.0 per request
            return 0.0

        # proceed with same lookup as before when in one of the allowed ranges
        phase = row.get("flight_phase")
        phase_s = str(phase) if not pd.isna(phase) else ""
        normalized = phase_s.strip().replace(" ", "_").lower()

        # determine type key
        type_val = None
        if type_col is not None:
            type_val = row.get(type_col)
        type_key = _resolve_type_key(type_val) if type_val is not None else None

        if type_key is None or type_key not in config:
            return pd.NA
        
        phase_to_section_lower = {k.lower(): v for k, v in phase_to_section.items()}
        section = phase_to_section_lower.get(normalized, "CR")
        try:
            sec_obj = config[type_key].get(section, {})
            val = sec_obj.get("CD0,deltaLDG") if isinstance(sec_obj, dict) else None
            if val is None:
                # fallback: some configs might have CD0,deltaLDG at top level
                val = config[type_key].get("CD0,deltaLDG")
            if val is None:
                # missing config value -> use 0.0 per request
                return 0.0
            try:
                return float(val)
            except Exception:
                return 0.0
        except Exception:
            return pd.NA

    df_out["CD0,deltaLDG"] = df_out.apply(_select_cd0_deltaLDG, axis=1)
    # cleanup helper column
    try:
        df_out.drop(columns=["_pos"], inplace=True)
    except Exception:
        pass
    # เพิ่มคอลัมน์ K: เลือกค่า K ตาม flight_phase และชนิดเครื่องบิน
    # กฏการแมป (ตามที่ผู้ใช้ระบุ):
    # taxi_out -> CR
    # takeoff -> IC
    # initial_climb -> IC
    # climb -> CR
    # cruise -> CR
    # descent -> CR
    # approach -> CR
    # landing -> LD
    # taxi_in -> CR
    k_phase_to_section = {
        "Taxi_out": "CR",
        "Takeoff": "IC",
        "Initial_climb": "IC",
        "Climb": "CR",
        "Cruise": "CR",
        "Descent": "CR",
        "Approach": "CR",
        "Landing": "LD",
        "Taxi_in": "CR",
    }

    def _select_k(row: pd.Series) -> object:
        phase = row.get("flight_phase")
        phase_s = str(phase) if not pd.isna(phase) else ""
        normalized = phase_s.strip().replace(" ", "_").lower()

        # determine type key
        type_val = None
        if type_col is not None:
            type_val = row.get(type_col)
        type_key = _resolve_type_key(type_val) if type_val is not None else None

        if type_key is None or type_key not in config:
            return pd.NA

        # case-insensitive lookup
        k_phase_to_section_lower = {k.lower(): v for k, v in k_phase_to_section.items()}
        section_code = k_phase_to_section_lower.get(normalized, "CR")

        code_to_name = {
            "CR": "cruise",
            "IC": "initial_climb",
            "LD": "landing",
            "TO": "takeoff",
            "AP": "approach",
        }

        # Build candidate names to search for in the config for this type_key
        candidates = [section_code, section_code.lower(), section_code.upper()]
        if section_code in code_to_name:
            candidates.extend([
                code_to_name[section_code],
                code_to_name[section_code].lower(),
                code_to_name[section_code].upper(),
            ])

        cfg_keys = list(config[type_key].keys())
        cfg_keys_lower = {k.lower(): k for k in cfg_keys}

        for cand in candidates:
            if not cand:
                continue
            cand_lower = cand.lower()
            if cand_lower in cfg_keys_lower:
                sec_key = cfg_keys_lower[cand_lower]
                sec_obj = config[type_key].get(sec_key, {})
                if isinstance(sec_obj, dict):
                    kval = sec_obj.get("K")
                    if kval is not None:
                        try:
                            return float(kval)
                        except Exception:
                            return pd.NA

        # fallback: try top-level K under the aircraft type
        topk = config[type_key].get("K")
        if topk is not None:
            try:
                return float(topk)
            except Exception:
                return pd.NA

        return pd.NA

    df_out["K"] = df_out.apply(_select_k, axis=1)

    # เพิ่มคอลัมน์ ROCD_m/s: rate of climb/descent (m/s)
    # วิธีคำนวณ: (altitude_current - altitude_previous) [ft] -> แปลงเป็น m แล้วหารด้วย delta_t (s)
    try:
        alt_ft = pd.to_numeric(df_out.get("altitude"), errors="coerce")
        alt_diff_ft = alt_ft.diff()
        delta_t_safe = df_out["delta_t (s)"].replace(0, pd.NA)
        df_out["ROCD_m/s"] = (alt_diff_ft * 0.3048) / delta_t_safe
        df_out["ROCD_m/s"] = df_out["ROCD_m/s"].replace([np.inf, -np.inf], pd.NA)
        # ensure first row is 0 (rate from previous nonexistent row)
        if len(df_out) > 0:
            try:
                df_out.at[df_out.index[0], "ROCD_m/s"] = 0.0
            except Exception:
                pass
    except Exception:
        df_out["ROCD_m/s"] = pd.NA

    # คำนวณ gamma_rad = arcsin(ROCD_m/s / TAS_m/s)
    try:
        rocd = pd.to_numeric(df_out.get("ROCD_m/s"), errors="coerce")
        tas = pd.to_numeric(df_out.get("TAS_m/s"), errors="coerce")
        ratio = rocd / tas
        # clamp values to [-1, 1] to avoid NaN from arcsin due to slight numeric errors
        ratio_clamped = ratio.clip(-1.0, 1.0)
        df_out["gamma_rad"] = np.arcsin(ratio_clamped)
    except Exception:
        df_out["gamma_rad"] = pd.NA
    return df_out
