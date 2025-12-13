from __future__ import annotations

import math
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
    
    return df_out


def detect_flight_phase(df: pd.DataFrame, alt_col: str = "altitude", track_col: str = "track") -> pd.DataFrame:
    """
    กำหนด flight phase สำหรับแต่ละแถวใน DataFrame
    
    Flight phases:
    - Taxi_out: altitude = 0 ft, track ไม่คงที่ (ช่วงแรกสุด, ก่อน Takeoff)
    - Takeoff: altitude = 0 ft, track คงที่/นิ่ง (อาจเพิ่มขึ้นเล็กน้อย) (หลัง Taxi_out, ก่อน Initial_climb)
    - Initial_climb: altitude > 0 ft ถึง 2000 ft (หลัง Takeoff)
    - Climb: altitude > 2000 ft ถึง (cruise_altitude - 1 ft) (หลัง Initial_climb)
    - Cruise: altitude คงที่นิ่ง (หลัง Climb)
    - Descent: altitude เปลี่ยนจาก Cruise ลงมา ถึง > 8000 ft (หลัง Cruise)
    - Approach: altitude 8000 ft ถึง 3000 ft (หลัง Descent)
    - Landing: altitude < 3000 ft ถึง 0 ft, track คงที่/ซ้ำเยอะ (หลัง Approach)
    - Taxi_in: altitude = 0 ft, track ไม่คงที่/เปลี่ยน (ช่วงท้ายสุด, หลัง Landing)
    
    Args:
        df: DataFrame ที่มีคอลัมน์ altitude และ track
        alt_col: ชื่อคอลัมน์ altitude (default: "altitude")
        track_col: ชื่อคอลัมน์ track (default: "track")
    
    Returns:
        DataFrame ที่เพิ่มคอลัมน์ "flight_phase"
    """
    df_out = df.copy()
    
    # แปลงคอลัมน์เป็น numeric
    altitude = pd.to_numeric(df_out[alt_col], errors="coerce")
    
    # ตรวจสอบว่ามีคอลัมน์ track หรือไม่
    if track_col not in df_out.columns:
        # ถ้าไม่มี track ให้ใช้ค่า NaN สำหรับการตรวจสอบ
        track = pd.Series([np.nan] * len(df_out))
    else:
        track = pd.to_numeric(df_out[track_col], errors="coerce")
    
    # เริ่มต้นด้วยค่า Unknown
    phases = pd.Series(["Unknown"] * len(df_out), dtype=object)
    
    # 1. หา Cruise Altitude (altitude ที่คงที่นิ่งเป็นเวลานาน)
    cruise_altitude = _detect_cruise_altitude(altitude)
    
    # 2. ตรวจสอบความคงที่ของ track (ใช้สำหรับแยก Landing/Taxi_in และ Taxi_out/Takeoff)
    track_stability = _calculate_track_stability(track, window_size=10)
    
    # 3. หาจุดเริ่มต้นและสิ้นสุดของแต่ละ phase
    # หาจุดที่ altitude เริ่มเปลี่ยนจาก 0
    first_non_zero_idx = altitude[altitude > 0].index
    if len(first_non_zero_idx) > 0:
        first_flying_idx = first_non_zero_idx[0]
    else:
        first_flying_idx = len(df_out)
    
    # หาจุดที่ altitude กลับมาเป็น 0 (หลังบิน)
    last_non_zero_idx = altitude[altitude > 0].index
    if len(last_non_zero_idx) > 0:
        last_flying_idx = last_non_zero_idx[-1]
    else:
        last_flying_idx = -1
    
    # 4. จำแนก phases
    for i in range(len(df_out)):
        alt = altitude.iloc[i]
        is_ground = (alt == 0) or (pd.isna(alt))
        
        if is_ground:
            # Ground phases: Taxi_out, Takeoff, Landing, Taxi_in
            if i < first_flying_idx:
                # ก่อนบินขึ้น: Taxi_out หรือ Takeoff
                if i < len(track_stability) and track_stability.iloc[i] < 0.7:
                    # track ไม่คงที่ -> Taxi_out
                    phases.iloc[i] = "Taxi_out"
                else:
                    # track คงที่ -> Takeoff
                    phases.iloc[i] = "Takeoff"
            elif i > last_flying_idx:
                # หลังบินลง: Landing หรือ Taxi_in
                if i < len(track_stability) and track_stability.iloc[i] < 0.7:
                    # track ไม่คงที่ -> Taxi_in
                    phases.iloc[i] = "Taxi_in"
                else:
                    # track คงที่ -> Landing
                    phases.iloc[i] = "Landing"
            else:
                # ระหว่างบิน แต่ altitude = 0 (อาจเป็นข้อมูลผิดพลาด)
                # ดูจากบริบทรอบๆ
                if i > 0 and altitude.iloc[i-1] > 0:
                    phases.iloc[i] = "Landing"
                elif i < len(df_out) - 1 and altitude.iloc[i+1] > 0:
                    phases.iloc[i] = "Takeoff"
                else:
                    phases.iloc[i] = "Unknown"
        else:
            # Flying phases
            # ตรวจสอบว่าเคยถึง Cruise แล้วหรือยัง (ดูจาก phases ก่อนหน้า)
            has_reached_cruise = False
            if i > 0:
                # ตรวจสอบว่าเคยมี Cruise phase มาก่อนหรือไม่
                prev_phases = phases.iloc[:i]
                has_reached_cruise = "Cruise" in prev_phases.values or "Descent" in prev_phases.values
            
            if cruise_altitude is not None and abs(alt - cruise_altitude) <= 100:
                # อยู่ในช่วง Cruise (ยอมให้เบี่ยงเบน ±100 ft)
                phases.iloc[i] = "Cruise"
            elif alt <= 2000:
                phases.iloc[i] = "Initial_climb"
            elif cruise_altitude is not None and alt < (cruise_altitude - 1):
                # ตรวจสอบว่าเป็น Descent หรือ Climb
                if has_reached_cruise and alt > 8000:
                    # ถ้าเคยถึง Cruise แล้ว และ altitude > 8000 -> Descent
                    phases.iloc[i] = "Descent"
                else:
                    phases.iloc[i] = "Climb"
            elif alt > 8000:
                # altitude > 8000
                if cruise_altitude is not None:
                    if has_reached_cruise:
                        # ถ้าเคยถึง Cruise แล้ว -> Descent
                        phases.iloc[i] = "Descent"
                    elif alt >= cruise_altitude:
                        # ถ้ายังไม่ถึง Cruise แต่ altitude >= cruise_altitude -> อาจเป็น Climb หรือ Cruise
                        if abs(alt - cruise_altitude) <= 100:
                            phases.iloc[i] = "Cruise"
                        else:
                            phases.iloc[i] = "Climb"
                    else:
                        # altitude > 8000 แต่ < cruise_altitude และยังไม่ถึง Cruise -> Climb
                        phases.iloc[i] = "Climb"
                else:
                    # ไม่มี cruise_altitude -> ใช้การตรวจสอบจาก phase ก่อนหน้า
                    if has_reached_cruise:
                        phases.iloc[i] = "Descent"
                    else:
                        phases.iloc[i] = "Climb"
            elif 3000 <= alt <= 8000:
                phases.iloc[i] = "Approach"
            elif alt < 3000:
                phases.iloc[i] = "Landing"
            else:
                phases.iloc[i] = "Unknown"
    
    # 5. ปรับปรุง phases ให้ต่อเนื่องและสมเหตุสมผล
    phases = _refine_flight_phases(phases, altitude, cruise_altitude)
    
    df_out["flight_phase"] = phases
    return df_out


def _detect_cruise_altitude(altitude: pd.Series, min_stable_rows: int = 30, tolerance_ft: float = 100.0) -> Optional[float]:
    """
    หา Cruise Altitude โดยดูหาช่วงที่ altitude คงที่นิ่งเป็นเวลานาน
    
    Args:
        altitude: Series ของ altitude values
        min_stable_rows: จำนวนแถวขั้นต่ำที่ต้องคงที่เพื่อนับเป็น Cruise
        tolerance_ft: ความยอมรับได้ของความแตกต่าง (feet)
    
    Returns:
        Cruise altitude (feet) หรือ None ถ้าไม่พบ
    """
    if len(altitude) < min_stable_rows:
        return None
    
    # หาช่วงที่ altitude คงที่
    for i in range(len(altitude) - min_stable_rows + 1):
        window = altitude.iloc[i:i+min_stable_rows]
        # ข้ามค่า NaN
        window_clean = window.dropna()
        if len(window_clean) < min_stable_rows * 0.8:  # ต้องมีข้อมูลอย่างน้อย 80%
            continue
        
        # ตรวจสอบว่าค่าใน window อยู่ในช่วง tolerance หรือไม่
        alt_mean = window_clean.mean()
        alt_std = window_clean.std()
        
        if alt_std <= tolerance_ft and alt_mean > 5000:  # Cruise มักจะสูงกว่า 5000 ft
            # ตรวจสอบว่าช่วงนี้ยาวพอหรือไม่
            # ขยาย window ไปข้างหน้าและข้างหลังเพื่อหาช่วงที่ยาวที่สุด
            start_idx = i
            end_idx = i + min_stable_rows
            
            # ขยายไปข้างหน้า
            while end_idx < len(altitude):
                if pd.isna(altitude.iloc[end_idx]):
                    end_idx += 1
                    continue
                if abs(altitude.iloc[end_idx] - alt_mean) <= tolerance_ft:
                    end_idx += 1
                else:
                    break
            
            # ขยายไปข้างหลัง
            while start_idx > 0:
                if pd.isna(altitude.iloc[start_idx - 1]):
                    start_idx -= 1
                    continue
                if abs(altitude.iloc[start_idx - 1] - alt_mean) <= tolerance_ft:
                    start_idx -= 1
                else:
                    break
            
            # ถ้าช่วงยาวพอ ให้คืนค่า mean
            if (end_idx - start_idx) >= min_stable_rows:
                return float(alt_mean)
    
    return None


def _calculate_track_stability(track: pd.Series, window_size: int = 10) -> pd.Series:
    """
    คำนวณความคงที่ของ track โดยดูว่าค่าใน window มีการเปลี่ยนแปลงมากหรือน้อย
    
    Returns:
        Series ของค่า stability (0-1, 1 = คงที่มาก, 0 = เปลี่ยนมาก)
    """
    stability = pd.Series([0.5] * len(track), dtype=float)
    
    for i in range(len(track)):
        start_idx = max(0, i - window_size // 2)
        end_idx = min(len(track), i + window_size // 2 + 1)
        window = track.iloc[start_idx:end_idx].dropna()
        
        if len(window) < 3:
            stability.iloc[i] = 0.5
            continue
        
        # คำนวณ standard deviation ของ track
        track_std = window.std()
        
        # ถ้า std น้อย แสดงว่าคงที่มาก
        # ใช้ threshold ที่เหมาะสม (เช่น std < 5 องศา = คงที่มาก)
        if track_std < 5:
            stability.iloc[i] = 1.0
        elif track_std < 15:
            stability.iloc[i] = 0.7
        elif track_std < 30:
            stability.iloc[i] = 0.4
        else:
            stability.iloc[i] = 0.1
    
    return stability


def _refine_flight_phases(phases: pd.Series, altitude: pd.Series, cruise_altitude: Optional[float]) -> pd.Series:
    """
    ปรับปรุง flight phases ให้ต่อเนื่องและสมเหตุสมผล
    
    - แก้ไข phases ที่ไม่ต่อเนื่อง
    - ตรวจสอบลำดับของ phases ให้ถูกต้อง
    """
    refined = phases.copy()
    
    # 1. แก้ไข phases ที่อยู่ระหว่าง Climb และ Cruise
    if cruise_altitude is not None:
        for i in range(1, len(refined) - 1):
            alt = altitude.iloc[i]
            if refined.iloc[i] == "Climb" and abs(alt - cruise_altitude) <= 100:
                # ถ้าใกล้ cruise altitude มาก ให้เปลี่ยนเป็น Cruise
                refined.iloc[i] = "Cruise"
            elif refined.iloc[i] == "Cruise" and alt < (cruise_altitude - 200):
                # ถ้า altitude ลดลงมากจาก cruise -> Descent
                refined.iloc[i] = "Descent"
    
    # 2. แก้ไข phases ที่กระโดดข้าม (เช่น จาก Climb ไป Landing โดยไม่มี Cruise/Descent/Approach)
    for i in range(1, len(refined)):
        prev_phase = refined.iloc[i-1]
        curr_phase = refined.iloc[i]
        
        # ถ้ากระโดดจาก Climb ไป Landing/Approach โดยไม่มี Cruise
        if prev_phase == "Climb" and curr_phase in ["Landing", "Approach"]:
            # ตรวจสอบ altitude
            if cruise_altitude is not None and altitude.iloc[i] < cruise_altitude:
                refined.iloc[i] = "Descent"
        
        # ถ้ากระโดดจาก Cruise ไป Landing โดยไม่มี Descent/Approach
        if prev_phase == "Cruise" and curr_phase == "Landing":
            alt = altitude.iloc[i]
            if alt > 8000:
                refined.iloc[i] = "Descent"
            elif alt > 3000:
                refined.iloc[i] = "Approach"
    
    # 3. แก้ไข phases ที่อยู่ติดกันให้ต่อเนื่อง
    for i in range(1, len(refined) - 1):
        prev_phase = refined.iloc[i-1]
        curr_phase = refined.iloc[i]
        next_phase = refined.iloc[i+1]
        
        # ถ้า phase กลางไม่ตรงกับ phase ก่อนและหลัง
        if prev_phase == next_phase and curr_phase != prev_phase:
            # ถ้าเป็น phase เดียวกันติดกัน ให้เปลี่ยน phase กลางให้เหมือนกัน
            if prev_phase != "Unknown":
                refined.iloc[i] = prev_phase
    
    return refined
