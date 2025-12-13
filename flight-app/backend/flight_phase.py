"""
Flight Phase Detection Module

โมดูลสำหรับกำหนด flight phase ของแต่ละแถวในข้อมูล ADS-B
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


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
    
    # 4. หาช่วง Cruise โดยตรงจากข้อมูล (ดูที่การซ้ำของค่า)
    cruise_start_idx = None
    cruise_end_idx = None
    if cruise_altitude is not None:
        # หาช่วงที่ altitude ใกล้เคียงกับ cruise_altitude
        altitude_rounded = (altitude / 100).round() * 100
        cruise_rounded = round(cruise_altitude / 100) * 100
        
        in_cruise = False
        for i in range(len(altitude_rounded)):
            if pd.isna(altitude_rounded.iloc[i]):
                continue
            if abs(altitude_rounded.iloc[i] - cruise_rounded) <= 50:
                if not in_cruise:
                    cruise_start_idx = i
                    in_cruise = True
                cruise_end_idx = i
            else:
                if in_cruise:
                    break
    
    # 5. จำแนก phases
    for i in range(len(df_out)):
        alt = altitude.iloc[i]
        is_ground = (alt == 0) or (pd.isna(alt)) or (alt < 10)  # ถือว่าเป็น ground ถ้า < 10 ft
        
        if is_ground:
            # Ground phases: Taxi_out, Takeoff, Landing, Taxi_in
            if i < first_flying_idx:
                # ก่อนบินขึ้น: Taxi_out หรือ Takeoff
                # ตรวจสอบ track stability ใน window ข้างๆ
                window_start = max(0, i - 5)
                window_end = min(len(track_stability), i + 6)
                window_stability = track_stability.iloc[window_start:window_end].mean()
                
                if window_stability < 0.6:
                    # track ไม่คงที่ -> Taxi_out
                    phases.iloc[i] = "Taxi_out"
                else:
                    # track คงที่ -> Takeoff
                    phases.iloc[i] = "Takeoff"
            elif i > last_flying_idx:
                # หลังบินลง: Landing หรือ Taxi_in
                # ตรวจสอบ track stability ใน window ข้างๆ
                window_start = max(0, i - 5)
                window_end = min(len(track_stability), i + 6)
                window_stability = track_stability.iloc[window_start:window_end].mean()
                
                if window_stability < 0.6:
                    # track ไม่คงที่ -> Taxi_in
                    phases.iloc[i] = "Taxi_in"
                else:
                    # track คงที่ -> Landing
                    phases.iloc[i] = "Landing"
            else:
                # ระหว่างบิน แต่ altitude = 0 (อาจเป็นข้อมูลผิดพลาด)
                # ดูจากบริบทรอบๆ
                if i > 0 and altitude.iloc[i-1] > 10:
                    phases.iloc[i] = "Landing"
                elif i < len(df_out) - 1 and altitude.iloc[i+1] > 10:
                    phases.iloc[i] = "Takeoff"
                else:
                    # ดูจาก phase ก่อนหน้า
                    if i > 0:
                        prev_phase = phases.iloc[i-1]
                        if prev_phase in ["Landing", "Approach"]:
                            phases.iloc[i] = "Landing"
                        elif prev_phase in ["Takeoff", "Initial_climb"]:
                            phases.iloc[i] = "Takeoff"
                        else:
                            phases.iloc[i] = "Unknown"
                    else:
                        phases.iloc[i] = "Unknown"
        else:
            # Flying phases
            # ตรวจสอบว่าเคยถึง Cruise แล้วหรือยัง (ดูจาก cruise_start_idx)
            has_reached_cruise = (cruise_start_idx is not None and i >= cruise_start_idx)
            has_passed_cruise = (cruise_end_idx is not None and i > cruise_end_idx)
            
            # ตรวจสอบว่าเป็น Cruise หรือไม่ (ดูที่การซ้ำของค่า)
            is_cruise = False
            if cruise_start_idx is not None and cruise_end_idx is not None:
                if cruise_start_idx <= i <= cruise_end_idx:
                    # ตรวจสอบว่า altitude ใกล้เคียงกับ cruise_altitude
                    altitude_rounded = round(alt / 100) * 100
                    cruise_rounded = round(cruise_altitude / 100) * 100
                    if abs(altitude_rounded - cruise_rounded) <= 50:
                        is_cruise = True
            
            if is_cruise:
                phases.iloc[i] = "Cruise"
            elif alt <= 2000:
                phases.iloc[i] = "Initial_climb"
            elif has_passed_cruise:
                # ผ่าน Cruise แล้ว -> ต้องเป็น Descent, Approach, หรือ Landing
                if alt > 8000:
                    phases.iloc[i] = "Descent"
                elif 3000 <= alt <= 8000:
                    phases.iloc[i] = "Approach"
                elif alt < 3000 and alt > 0:
                    phases.iloc[i] = "Landing"
                else:
                    phases.iloc[i] = "Unknown"
            elif has_reached_cruise:
                # ยังอยู่ในช่วง Cruise หรือใกล้ Cruise
                if abs(alt - cruise_altitude) <= 100:
                    phases.iloc[i] = "Cruise"
                else:
                    phases.iloc[i] = "Climb"
            elif alt > 8000:
                # ยังไม่ถึง Cruise และ altitude > 8000 -> Climb
                phases.iloc[i] = "Climb"
            elif cruise_altitude is not None and alt < cruise_altitude:
                # ยังไม่ถึง Cruise และ altitude < cruise_altitude -> Climb
                phases.iloc[i] = "Climb"
            elif 3000 <= alt <= 8000:
                # ตรวจสอบว่าเป็น Approach หรือ Climb
                if has_passed_cruise or (i > 0 and phases.iloc[i-1] in ["Descent", "Approach"]):
                    phases.iloc[i] = "Approach"
                else:
                    phases.iloc[i] = "Climb"
            elif alt < 3000 and alt > 0:
                # ตรวจสอบว่าเป็น Landing หรือ Initial_climb
                if i > 0 and phases.iloc[i-1] in ["Approach", "Descent", "Landing"]:
                    phases.iloc[i] = "Landing"
                else:
                    phases.iloc[i] = "Initial_climb"
            else:
                phases.iloc[i] = "Unknown"
    
    # 6. ปรับปรุง phases ให้ต่อเนื่องและสมเหตุสมผล
    phases = _refine_flight_phases(phases, altitude, cruise_altitude)
    
    df_out["flight_phase"] = phases
    return df_out


def _detect_cruise_altitude(altitude: pd.Series, min_stable_rows: int = 30, tolerance_ft: float = 50.0) -> Optional[float]:
    """
    หา Cruise Altitude โดยดูหาช่วงที่ altitude ซ้ำตัวเดิมซ้ำต่อกันไปเรื่อยๆ
    
    Args:
        altitude: Series ของ altitude values
        min_stable_rows: จำนวนแถวขั้นต่ำที่ต้องซ้ำเพื่อนับเป็น Cruise
        tolerance_ft: ความยอมรับได้ของความแตกต่าง (feet) - ใช้สำหรับปัดเศษ
    
    Returns:
        Cruise altitude (feet) หรือ None ถ้าไม่พบ
    """
    if len(altitude) < min_stable_rows:
        return None
    
    # ปัดเศษ altitude เพื่อหาค่าที่ซ้ำกัน (ปัดเป็น 100 ft)
    altitude_rounded = (altitude / 100).round() * 100
    
    # หาช่วงที่ altitude ซ้ำต่อเนื่องกัน
    best_cruise_alt = None
    best_length = 0
    best_start = 0
    best_end = 0
    
    i = 0
    while i < len(altitude_rounded) - min_stable_rows:
        if pd.isna(altitude_rounded.iloc[i]) or altitude.iloc[i] < 5000:
            i += 1
            continue
        
        # หาค่า altitude ที่ซ้ำในจุดนี้
        current_alt = altitude_rounded.iloc[i]
        start_idx = i
        
        # นับจำนวนแถวที่ซ้ำต่อเนื่องกัน
        j = i
        while j < len(altitude_rounded):
            if pd.isna(altitude_rounded.iloc[j]):
                j += 1
                continue
            # ตรวจสอบว่าค่าใกล้เคียงกันหรือไม่ (ภายใน tolerance)
            if abs(altitude_rounded.iloc[j] - current_alt) <= tolerance_ft:
                j += 1
            else:
                break
        
        # ถ้าช่วงยาวพอ
        length = j - start_idx
        if length >= min_stable_rows:
            # คำนวณค่าเฉลี่ยของ altitude จริง (ไม่ใช่ rounded)
            cruise_window = altitude.iloc[start_idx:j].dropna()
            if len(cruise_window) > 0:
                cruise_mean = float(cruise_window.mean())
                # ถ้าช่วงนี้ยาวกว่าช่วงที่เจอมาก่อน ให้อัพเดท
                if length > best_length:
                    best_length = length
                    best_cruise_alt = cruise_mean
                    best_start = start_idx
                    best_end = j
        
        # ข้ามไปยังจุดที่ค่าเปลี่ยน
        i = j
    
    return best_cruise_alt


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
    - แก้ไข Landing และ Takeoff ให้ถูกต้อง
    """
    refined = phases.copy()
    
    # 1. แก้ไข Landing phase - ต้องเป็นช่วงที่ altitude ลดลงจาก 3000 ถึง 0
    for i in range(len(refined)):
        alt = altitude.iloc[i]
        if alt < 3000 and alt > 0:
            # ตรวจสอบว่าเป็น Landing หรือไม่ (ดูจาก phase ก่อนหน้า)
            if i > 0:
                prev_phase = refined.iloc[i-1]
                if prev_phase in ["Approach", "Descent", "Landing"]:
                    refined.iloc[i] = "Landing"
                elif prev_phase in ["Initial_climb", "Climb"]:
                    # ยังไม่ควรเป็น Landing
                    pass
    
    # 2. แก้ไข Takeoff phase - ต้องเป็นช่วงที่ altitude = 0 ก่อน Initial_climb
    for i in range(len(refined) - 1):
        if refined.iloc[i] == "Takeoff" and altitude.iloc[i] == 0:
            # ตรวจสอบว่า phase ถัดไปเป็น Initial_climb หรือไม่
            if i + 1 < len(refined) and refined.iloc[i+1] == "Initial_climb":
                # ถูกต้องแล้ว
                pass
            elif i + 1 < len(refined) and altitude.iloc[i+1] > 0:
                # ถ้าแถวถัดไปมี altitude > 0 แต่ phase ไม่ใช่ Initial_climb
                # อาจต้องแก้ไข
                pass
    
    # 3. แก้ไข phases ที่อยู่ระหว่าง Climb และ Cruise
    if cruise_altitude is not None:
        for i in range(1, len(refined) - 1):
            alt = altitude.iloc[i]
            if refined.iloc[i] == "Climb" and abs(alt - cruise_altitude) <= 50:
                # ถ้าใกล้ cruise altitude มาก ให้เปลี่ยนเป็น Cruise
                # แต่ต้องตรวจสอบว่ายังไม่เคยถึง Cruise
                if i > 0 and "Cruise" not in refined.iloc[:i].values:
                    refined.iloc[i] = "Cruise"
            elif refined.iloc[i] == "Cruise" and alt < (cruise_altitude - 200):
                # ถ้า altitude ลดลงมากจาก cruise -> Descent
                refined.iloc[i] = "Descent"
    
    # 4. แก้ไข phases ที่กระโดดข้าม (เช่น จาก Climb ไป Landing โดยไม่มี Cruise/Descent/Approach)
    for i in range(1, len(refined)):
        prev_phase = refined.iloc[i-1]
        curr_phase = refined.iloc[i]
        alt = altitude.iloc[i]
        
        # ถ้ากระโดดจาก Climb ไป Landing/Approach โดยไม่มี Cruise
        if prev_phase == "Climb" and curr_phase in ["Landing", "Approach"]:
            # ตรวจสอบ altitude
            if cruise_altitude is not None and alt < cruise_altitude:
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
        
        # ถ้ากระโดดจาก Cruise ไป Landing โดยไม่มี Descent/Approach
        if prev_phase == "Cruise" and curr_phase == "Landing":
            if alt > 8000:
                refined.iloc[i] = "Descent"
            elif alt > 3000:
                refined.iloc[i] = "Approach"
    
    # 5. แก้ไข phases ที่อยู่ติดกันให้ต่อเนื่อง (แต่ไม่เปลี่ยน Landing/Takeoff)
    for i in range(1, len(refined) - 1):
        prev_phase = refined.iloc[i-1]
        curr_phase = refined.iloc[i]
        next_phase = refined.iloc[i+1]
        
        # ถ้า phase กลางไม่ตรงกับ phase ก่อนและหลัง
        if prev_phase == next_phase and curr_phase != prev_phase:
            # ถ้าเป็น phase เดียวกันติดกัน ให้เปลี่ยน phase กลางให้เหมือนกัน
            # แต่ไม่เปลี่ยน Landing/Takeoff ถ้า altitude = 0
            if prev_phase != "Unknown":
                if altitude.iloc[i] == 0 and curr_phase in ["Landing", "Takeoff"]:
                    # ไม่เปลี่ยน Landing/Takeoff ที่ altitude = 0
                    pass
                else:
                    refined.iloc[i] = prev_phase
    
    return refined

