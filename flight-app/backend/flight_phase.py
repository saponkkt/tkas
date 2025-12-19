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
    - Takeoff: altitude = 0 ft, และค่า track/Direction คงที่ (เป็นค่าเดียวกันหรือใกล้เคียงกัน) ซ้ำกันมากกว่า 3 แถว
      โดยช่วงนี้ต้องอยู่ "หลัง Taxi_out" และ "ก่อน Initial_climb" (ช่วงก่อนบรรทัดแรกของ Initial_climb)
    - Initial_climb: altitude > 0 ft ถึง 2000 ft เฉพาะช่วงที่เกิด "หลัง Takeoff" และ "ก่อน Climb"
    - Climb: altitude > 2000 ft ถึง (cruise_altitude - 1 ft) (หลัง Initial_climb)
    - Cruise: ช่วงที่ altitude สูงที่สุด และมีค่าซ้ำต่อกันมากกว่า 5 แถว (หลัง Climb และก่อน Descent)
    - Descent: altitude เปลี่ยนจาก Cruise ลงมา ถึง > 8000 ft (หลัง Cruise และหลังช่วง Cruise ที่เป็น altitude สูงสุด)
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
    
    # ตรวจสอบว่ามีคอลัมน์ track หรือไม่ ถ้าไม่มีลองหา Direction
    if track_col in df_out.columns:
        track = pd.to_numeric(df_out[track_col], errors="coerce")
    elif "Direction" in df_out.columns:
        track = pd.to_numeric(df_out["Direction"], errors="coerce")
    else:
        # ถ้าไม่มี track หรือ Direction ให้ใช้ค่า NaN สำหรับการตรวจสอบ
        track = pd.Series([np.nan] * len(df_out))

    # ในช่วงก่อนบิน (ground) เราจะยังไม่ตัดสินว่าเป็น Taxi_out หรือ Takeoff
    # ตัดสินใจช่วง Takeoff โดยย้อนจาก `Initial_climb` ในภายหลัง
    
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
        # ใช้การปัดลง (floor) แทนการปัดปกติ เพื่อหลีกเลี่ยงการปัดค่าที่ขึ้นมาเป็นค่า cruise
        altitude_rounded = np.floor(altitude / 100) * 100
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
                # ก่อนบินขึ้น: ยังไม่กำหนดเป็น Taxi_out หรือ Takeoff ที่ระดับนี้
                # จะกำหนดช่วง Takeoff โดยย้อนจาก Initial_climb ภายหลัง
                phases.iloc[i] = "Unknown"
            elif i > last_flying_idx:
                # หลังบินลง: Landing หรือ Taxi_in
                # ตรวจสอบ phase ก่อนหน้าเพื่อตัดสินใจ
                if i > 0:
                    prev_phase = phases.iloc[i-1]
                    # ถ้า phase ก่อนหน้าเป็น Landing/Approach/Descent ให้เป็น Landing ต่อก่อน
                    if prev_phase in ["Landing", "Approach", "Descent"]:
                        # ตรวจสอบ track stability ใน window ข้างๆ
                        window_start = max(0, i - 5)
                        window_end = min(len(track_stability), i + 6)
                        window_stability = track_stability.iloc[window_start:window_end].mean()
                        
                        # นับจำนวน Landing ต่อเนื่องกันก่อนหน้านี้
                        landing_count = 0
                        for j in range(i-1, max(-1, i-20), -1):
                            if j >= 0 and phases.iloc[j] == "Landing":
                                landing_count += 1
                            else:
                                break
                        
                        # ถ้า track ไม่คงที่มาก (stability < 0.3) และมี Landing ต่อเนื่องกันมานานแล้ว (>= 5 แถว)
                        # ให้เปลี่ยนเป็น Taxi_in
                        if window_stability < 0.3 and landing_count >= 5:
                            phases.iloc[i] = "Taxi_in"
                        else:
                            # ยังเป็น Landing ต่อ
                            phases.iloc[i] = "Landing"
                    elif prev_phase == "Taxi_in":
                        # ถ้า phase ก่อนหน้าเป็น Taxi_in แล้ว ให้เป็น Taxi_in ต่อ
                        phases.iloc[i] = "Taxi_in"
                    else:
                        # กรณีอื่นๆ (เช่น Unknown) ให้ตรวจสอบ track stability
                        window_start = max(0, i - 5)
                        window_end = min(len(track_stability), i + 6)
                        window_stability = track_stability.iloc[window_start:window_end].mean()
                        
                        if window_stability < 0.3:
                            # track ไม่คงที่มาก -> Taxi_in
                            phases.iloc[i] = "Taxi_in"
                        else:
                            # track คงที่ -> Landing
                            phases.iloc[i] = "Landing"
                else:
                    # ถ้าเป็นแถวแรก ให้ตรวจสอบ track stability
                    window_start = max(0, i - 5)
                    window_end = min(len(track_stability), i + 6)
                    window_stability = track_stability.iloc[window_start:window_end].mean()
                    
                    if window_stability < 0.3:
                        phases.iloc[i] = "Taxi_in"
                    else:
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
                        # ตรวจสอบว่า altitude ใกล้เคียงกับ cruise_altitude (ใช้การปัดลงแบบเดียวกับการหาช่วง)
                        altitude_rounded = np.floor(alt / 100) * 100
                        cruise_rounded = np.floor(cruise_altitude / 100) * 100
                        if abs(altitude_rounded - cruise_rounded) <= 50:
                            is_cruise = True
            
            if is_cruise:
                phases.iloc[i] = "Cruise"
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
            elif alt < 2000 and alt > 0:
                # ช่วงต่ำกว่า 2000 ft ก่อนถึง Cruise
                # ต้องเป็น Initial_climb (ต่อจาก Takeoff/Taxi_out) หรือ Landing (จากด้านบนลงมา)
                if i > 0 and phases.iloc[i-1] in ["Approach", "Descent", "Landing"]:
                    # มาจากด้านบนเพื่อจะลงพื้น -> Landing
                    phases.iloc[i] = "Landing"
                else:
                    # หลัง Takeoff/Taxi_out/Initial_climb หรือไม่เข้าเคสชัดเจน -> Initial_climb
                    phases.iloc[i] = "Initial_climb"
            elif 3000 <= alt <= 8000:
                # ช่วง 3000–8000 ft ก่อนถึง Cruise: แยก Climb vs Approach ตามบริบท
                if has_passed_cruise or (i > 0 and phases.iloc[i-1] in ["Descent", "Approach"]):
                    phases.iloc[i] = "Approach"
                else:
                    phases.iloc[i] = "Climb"
            elif alt > 8000:
                # ยังไม่ถึง Cruise และ altitude > 8000 -> Climb
                phases.iloc[i] = "Climb"
            elif cruise_altitude is not None and alt < cruise_altitude:
                # ยังไม่ถึง Cruise และ altitude ต่ำกว่า cruise_altitude แต่ไม่เข้า band อื่น -> Climb
                phases.iloc[i] = "Climb"
            else:
                phases.iloc[i] = "Unknown"

    # 6. ปรับปรุงช่วง Takeoff ให้ตรงกับนิยามใหม่
    #    - หา Initial_climb ก่อน
    #    - ก่อนบรรทัดแรกของ Initial_climb ให้หาช่วงที่ altitude = 0 และค่า track/Direction ซ้ำกัน > 3 แถว
    #      ช่วงนั้นจะถูกกำหนดให้เป็น Takeoff และช่วงก่อนหน้าให้เป็น Taxi_out
    phases = _apply_takeoff_from_initial_climb(phases, altitude, track)

    # 6b. กำหนด Taxi_out เป็นทุกแถวก่อนเริ่ม Takeoff
    if "Takeoff" in phases.values:
        first_takeoff_idx_arr = np.where(phases == "Takeoff")[0]
        if len(first_takeoff_idx_arr) > 0:
            first_takeoff_idx = int(first_takeoff_idx_arr[0])
            if first_takeoff_idx > 0:
                phases.iloc[:first_takeoff_idx] = "Taxi_out"
    else:
        # ถ้าไม่พบ Takeoff ให้กำหนดช่วงก่อนบิน (ก่อน first_flying_idx) เป็น Taxi_out
        if first_flying_idx is not None and first_flying_idx > 0:
            phases.iloc[:first_flying_idx] = "Taxi_out"

    # 7. ปรับปรุง phases ให้ต่อเนื่องและสมเหตุสมผล
    phases = _refine_flight_phases(phases, altitude, cruise_altitude, cruise_start_idx, cruise_end_idx)
    
    # 8. ปรับปรุง Landing/Taxi_in ตาม Direction ที่ altitude = 0 ft
    phases = _refine_landing_taxi_in_by_direction(phases, altitude, track, tolerance_deg=5.0)
    
    df_out["flight_phase"] = phases
    return df_out


def _detect_cruise_altitude(altitude: pd.Series, min_stable_rows: int = 6, tolerance_ft: float = 50.0) -> Optional[float]:
    """
    หา Cruise Altitude โดยกำหนดให้:
    - Cruise คือช่วงที่มีค่า altitude สูงที่สุด (maximum altitude)
    - ค่า altitude (หลังปัด/ปัดลงเป็นช่วงๆ) ต้องซ้ำต่อกันอย่างน้อย min_stable_rows แถว
    
    Args:
        altitude: Series ของ altitude values
        min_stable_rows: จำนวนแถวขั้นต่ำที่ต้องซ้ำเพื่อนับเป็น Cruise (ต้อง > 5 แถว)
        tolerance_ft: ความยอมรับได้ของความแตกต่าง (feet) - ใช้สำหรับปัดเศษ
    
    Returns:
        Cruise altitude (feet) หรือ None ถ้าไม่พบ
    """
    if len(altitude) < min_stable_rows:
        return None

    # ปัด altitude เป็นช่วง 100 ft และใช้ค่า "สูงสุด" ของช่วงเหล่านั้น
    altitude_rounded = np.floor(altitude / 100) * 100

    # หา altitude สูงสุด (หลังปัด)
    if altitude_rounded.dropna().empty:
        return None
    max_alt = float(altitude_rounded.max(skipna=True))

    # หาช่วงที่ altitude (หลังปัด) เท่ากับ max_alt ต่อเนื่องกันอย่างน้อย min_stable_rows แถว
    best_start = None
    best_end = None
    best_length = 0

    i = 0
    n = len(altitude_rounded)
    while i < n:
        if pd.isna(altitude_rounded.iloc[i]) or abs(altitude_rounded.iloc[i] - max_alt) > tolerance_ft:
            i += 1
            continue

        start_idx = i
        while i < n and not pd.isna(altitude_rounded.iloc[i]) and abs(altitude_rounded.iloc[i] - max_alt) <= tolerance_ft:
            i += 1
        end_idx = i

        length = end_idx - start_idx
        if length > best_length:
            best_length = length
            best_start = start_idx
            best_end = end_idx

    # ถ้าไม่มีช่วงที่ยาวพอ ถือว่าไม่มี Cruise
    if best_length < min_stable_rows or best_start is None or best_end is None:
        return None

    # ใช้ค่าเฉลี่ยของ altitude จริงในช่วง Cruise ที่สูงที่สุด
    cruise_window = altitude.iloc[best_start:best_end].dropna()
    if cruise_window.empty:
        return None

    return float(cruise_window.mean())


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


def _apply_takeoff_from_initial_climb(
    phases: pd.Series,
    altitude: pd.Series,
    track: pd.Series,
    min_takeoff_rows: int = 1,
    track_tolerance_deg: float = 5.0,
) -> pd.Series:
    """
    ปรับปรุงช่วง Takeoff ตามนิยาม:
    - หา "ช่วง Initial_climb" ก่อน (phase = Initial_climb)
    - ก่อนบรรทัดแรกของแต่ละช่วง Initial_climb:
        - ดูค่าของ track/Direction ที่บรรทัดนั้น (target_track)
        - ไล่ย้อนขึ้นไปทีละบรรทัดในอดีต (ย้อนหลัง):
            - altitude ต้องเป็น 0
            - track/Direction ต้องมีค่าเท่ากัน หรือแตกต่างไม่เกิน track_tolerance_deg
        - ถ้าช่วงที่เข้าเงื่อนไขมีความยาว >= min_takeoff_rows
          ให้ช่วงนั้นเป็น Takeoff ทั้งหมด
          และบรรทัดก่อนหน้า Takeoff (ถ้าเป็น Takeoff เดิม) ให้ปรับกลับเป็น Taxi_out
    """
    refined = phases.copy()

    if len(refined) == 0:
        return refined

    # หา index ของทุกช่วง Initial_climb
    initial_mask = refined == "Initial_climb"
    if not initial_mask.any():
        return refined

    initial_indices = np.where(initial_mask.to_numpy())[0]

    # หา segment ต่อเนื่องของ Initial_climb
    segments: list[tuple[int, int]] = []
    start = initial_indices[0]
    prev = start
    for idx in initial_indices[1:]:
        if idx == prev + 1:
            prev = idx
        else:
            segments.append((start, prev))
            start = idx
            prev = idx
    segments.append((start, prev))

    n = len(refined)

    for seg_start, _seg_end in segments:
        ic_first_idx = seg_start
        if ic_first_idx <= 0:
            continue

        # ใช้ Direction ของแถวก่อนช่วง Initial_climb เป็นเกณฑ์
        ref_idx = ic_first_idx - 1
        if ref_idx < 0:
            continue
        target_track = track.iloc[ref_idx]
        if pd.isna(target_track):
            continue

        # ไล่ย้อนขึ้นไปหาช่วง Takeoff candidate โดยเช็คเฉพาะ altitude == 0 และทิศทาง
        j = ref_idx
        count = 0
        while j >= 0:
            alt_j = altitude.iloc[j]
            trk_j = track.iloc[j]
            if alt_j != 0 or pd.isna(trk_j):
                break
            if abs(trk_j - target_track) > track_tolerance_deg:
                # เจอแถวที่ทิศทางต่างเกิน tolerance -> หยุดย้อน (แถวนี้และก่อนหน้าจะเป็น Taxi_out)
                break
            count += 1
            j -= 1

        if count >= min_takeoff_rows:
            takeoff_start = ic_first_idx - count
            takeoff_end = ic_first_idx - 1

            # ปรับ phase ช่วง takeoff ให้เป็น Takeoff
            for k in range(takeoff_start, takeoff_end + 1):
                refined.iloc[k] = "Takeoff"

            # ช่วงก่อนหน้า takeoff ถ้ามี Takeoff เกินออกไป ให้เปลี่ยนกลับเป็น Taxi_out
            for k in range(0, takeoff_start):
                if refined.iloc[k] == "Takeoff":
                    refined.iloc[k] = "Taxi_out"
    return refined


def _refine_landing_taxi_in_by_direction(
    phases: pd.Series,
    altitude: pd.Series,
    track: pd.Series,
    tolerance_deg: float = 5.0,
) -> pd.Series:
    """
    ปรับปรุง Landing/Taxi_in ตาม Direction ที่ altitude = 0 ft:
    
    - หาแถวแรกที่ altitude = 0 ft หลังบินลง (ref_direction = Direction ของแถวนั้น)
    - สำหรับแถวที่ altitude = 0 ft:
      - ถ้า Direction ต่างจาก ref_direction ไม่เกิน ±tolerance_deg → Landing
      - ถ้า Direction ต่างจาก ref_direction เกิน ±tolerance_deg → Taxi_in
    - ตรวจสอบแถวก่อนหน้าแถวแรกที่ altitude = 0 ft ด้วย:
      - ถ้า Direction ต่างจาก ref_direction ไม่เกิน ±tolerance_deg → Landing
      - ถ้า Direction ต่างจาก ref_direction เกิน ±tolerance_deg → Taxi_in
    """
    refined = phases.copy()
    
    # หาจุดที่ altitude กลับมาเป็น 0 (หลังบิน)
    last_non_zero_idx = altitude[altitude > 0].index
    if len(last_non_zero_idx) == 0:
        return refined
    
    last_flying_idx = last_non_zero_idx[-1]
    
    # หาแถวแรกที่ altitude = 0 ft หลังบินลง
    first_zero_after_flight_idx = None
    for i in range(last_flying_idx + 1, len(altitude)):
        if altitude.iloc[i] == 0 or (pd.isna(altitude.iloc[i]) == False and altitude.iloc[i] < 10):
            first_zero_after_flight_idx = i
            break
    
    if first_zero_after_flight_idx is None:
        return refined
    
    # ใช้ Direction ของแถวแรกที่ altitude = 0 ft เป็น ref_direction
    ref_direction = track.iloc[first_zero_after_flight_idx]
    if pd.isna(ref_direction):
        return refined
    
    # ตรวจสอบแถวก่อนหน้าแถวแรกที่ altitude = 0 ft (ถ้ามี altitude < 3000 ft และ > 0 ft)
    if first_zero_after_flight_idx > 0:
        prev_idx = first_zero_after_flight_idx - 1
        prev_alt = altitude.iloc[prev_idx]
        if prev_alt < 3000 and prev_alt > 0:
            # ตรวจสอบว่า phase ก่อนหน้าเป็น Approach/Descent/Landing หรือไม่
            if prev_idx > 0:
                prev_phase = refined.iloc[prev_idx - 1]
                if prev_phase in ["Approach", "Descent", "Landing"]:
                    # ตรวจสอบ Direction
                    prev_direction = track.iloc[prev_idx]
                    if not pd.isna(prev_direction):
                        # คำนวณความต่างของ Direction (จัดการกรณีที่ Direction วนรอบ 0-360)
                        diff = abs(prev_direction - ref_direction)
                        if diff > 180:
                            diff = 360 - diff
                        
                        if diff <= tolerance_deg:
                            # Direction ใกล้เคียงกับ ref_direction → Landing
                            refined.iloc[prev_idx] = "Landing"
                        else:
                            # Direction ต่างจาก ref_direction เกิน tolerance → Taxi_in
                            refined.iloc[prev_idx] = "Taxi_in"
    
    # ตรวจสอบแถวที่ altitude = 0 ft ทั้งหมด
    # สร้างรายการแถว ground ต่อเนื่องตั้งแต่ first_zero_after_flight_idx
    ground_indices = []
    for i in range(first_zero_after_flight_idx, len(altitude)):
        alt = altitude.iloc[i]
        is_ground = (alt == 0) or (pd.isna(alt) == False and alt < 10)
        if not is_ground:
            break
        ground_indices.append(i)

    if not ground_indices:
        return refined

    # ตรวจสอบว่าแถวก่อนหน้า block เป็น Approach/Descent/Landing หากมี
    before_idx = ground_indices[0] - 1
    prev_phase_ok = False
    if before_idx >= 0:
        prev_phase = refined.iloc[before_idx]
        if prev_phase in ["Approach", "Descent", "Landing"]:
            prev_phase_ok = True

    # คำนวณความต่างทิศทางของแต่ละแถวใน block เทียบกับ ref_direction
    diffs = []
    for i in ground_indices:
        curr_direction = track.iloc[i]
        if pd.isna(curr_direction):
            diffs.append(float('inf'))
            continue
        diff = abs(curr_direction - ref_direction)
        if diff > 180:
            diff = 360 - diff
        diffs.append(diff)

    # ถ้าแถวก่อนหน้าไม่ใช่ Approach/Descent/Landing ให้พิจารณาเฉพาะการกำหนด Taxi_in
    if not prev_phase_ok:
        for i in ground_indices:
            refined.iloc[i] = "Taxi_in"
        return refined

    # หา index แรกที่ diff > tolerance => เริ่มเป็น Taxi_in ที่ตำแหน่งนั้น
    first_bad = None
    for idx, d in enumerate(diffs):
        if d > tolerance_deg:
            first_bad = idx
            break

    if first_bad is None:
        # ทั้งหมดใกล้เคียง → ทั้งหมดเป็น Landing
        for i in ground_indices:
            refined.iloc[i] = "Landing"
    else:
        # แถวก่อน first_bad เป็น Landing
        for j in range(0, first_bad):
            refined.iloc[ground_indices[j]] = "Landing"
        # ตั้งแต่ first_bad เป็นต้นไปเป็น Taxi_in
        for j in range(first_bad, len(ground_indices)):
            refined.iloc[ground_indices[j]] = "Taxi_in"
    
    return refined


def _refine_flight_phases(phases: pd.Series, altitude: pd.Series, cruise_altitude: Optional[float], cruise_start_idx: Optional[int] = None, cruise_end_idx: Optional[int] = None) -> pd.Series:
    """
    ปรับปรุง flight phases ให้ต่อเนื่องและสมเหตุสมผล
    
    - แก้ไข phases ที่ไม่ต่อเนื่อง
    - ตรวจสอบลำดับของ phases ให้ถูกต้อง
    - แก้ไข Landing และ Takeoff ให้ถูกต้อง
    """
    refined = phases.copy()
    
    # 1. แก้ไข Landing phase - ต้องเป็นช่วงที่ altitude ลดลงจาก 3000 ถึง 0 และต่อจาก Approach
    for i in range(len(refined)):
        alt = altitude.iloc[i]
        if alt < 3000 and alt > 0:
            # ตรวจสอบว่าเป็น Landing หรือไม่ (ดูจาก phase ก่อนหน้า)
            if i > 0:
                prev_phase = refined.iloc[i-1]
                if prev_phase in ["Approach", "Descent", "Landing"]:
                    # Landing ต้องต่อจาก Approach/Descent/Landing
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
                # แต่แค่เปลี่ยนถ้าอยู่ในช่วง cruise ที่ตรวจพบแล้ว (i >= cruise_start_idx)
                # หรือถ้ามี Cruise ปรากฏก่อนหน้านี้
                if (cruise_start_idx is not None and i >= cruise_start_idx) or ("Cruise" in refined.iloc[:i].values):
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
    
        # 6. บังคับลำดับเฟสให้ไม่ย้อนกลับ (monotonic phases)
        # ลำดับเฟสที่ยอมรับ
        phase_order = [
            "Taxi_out",
            "Takeoff",
            "Initial_climb",
            "Climb",
            "Cruise",
            "Descent",
            "Approach",
            "Landing",
            "Taxi_in",
        ]
        phase_to_idx = {p: i for i, p in enumerate(phase_order)}

        max_idx = -1
        for i in range(len(refined)):
            p = refined.iloc[i]
            idx = phase_to_idx.get(p, None)
            if idx is None:
                # Unknown or unexpected phase: keep current max if applicable
                if max_idx >= 0:
                    refined.iloc[i] = phase_order[max_idx]
            else:
                if idx < max_idx:
                    # ถ้าเฟสย้อนกลับ ให้เปลี่ยนเป็นเฟสสูงสุดที่เคยเจอ
                    refined.iloc[i] = phase_order[max_idx]
                else:
                    max_idx = idx

        return refined

