"""
Flight Phase Detection Module

โมดูลสำหรับกำหนด flight phase ของแต่ละแถวในข้อมูล ADS-B
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def detect_flight_phase(df: pd.DataFrame, alt_col: str = "altitude", track_col: str = "track", rocd_col: str = "ROCD_m/s") -> pd.DataFrame:
    """
    กำหนด flight phase สำหรับแต่ละแถวใน DataFrame
    
    Flight phases:
    - Taxi_out: altitude = 0 ft, track ไม่คงที่ (ช่วงแรกสุด, ก่อน Takeoff)
    - Takeoff: altitude = 0 ft, และค่า track/Direction คงที่ (เป็นค่าเดียวกันหรือใกล้เคียงกัน) ซ้ำกันมากกว่า 3 แถว
      โดยช่วงนี้ต้องอยู่ "หลัง Taxi_out" และ "ก่อน Initial_climb" (ช่วงก่อนบรรทัดแรกของ Initial_climb)
    - Initial_climb: altitude > 0 ft ถึง 2000 ft เฉพาะช่วงที่เกิด "หลัง Takeoff" และ "ก่อน Climb"
    - Climb: altitude > 2000 ft ถึง (cruise_altitude - 1 ft) (หลัง Initial_climb)
    - Cruise: altitude คงที่ AND ROCD_m/s ≈ 0 (หลัง Climb และก่อน Descent)
    - Descent: altitude เปลี่ยนจาก Cruise ลงมา ถึง > 8000 ft (หลัง Cruise)
    - Approach: altitude 8000 ft ถึง 3000 ft (หลัง Descent)
    - Landing: altitude < 3000 ft ถึง 0 ft, track คงที่/ซ้ำเยอะ (หลัง Approach)
    - Taxi_in: altitude = 0 ft, track ไม่คงที่/เปลี่ยน (ช่วงท้ายสุด, หลัง Landing)
    
    Args:
        df: DataFrame ที่มีคอลัมน์ altitude และ track
        alt_col: ชื่อคอลัมน์ altitude (default: "altitude")
        track_col: ชื่อคอลัมน์ track (default: "track")
        rocd_col: ชื่อคอลัมน์ ROCD_m/s (default: "ROCD_m/s")
    
    Returns:
        DataFrame ที่เพิ่มคอลัมน์ "flight_phase"
    """
    df_out = df.copy()
    
    # แปลงคอลัมน์เป็น numeric
    altitude = pd.to_numeric(df_out[alt_col], errors="coerce")

    # สร้างเวอร์ชัน smoothed ของ altitude เพื่อป้องกันการกระโดดชั่วคราว
    # ใช้ rolling median ขนาดเล็ก (3 แถว) ที่อยู่ตรงกลางเพื่อลด noise สั้นๆ
    altitude_smoothed = altitude.rolling(window=3, center=True, min_periods=1).median()
    # เติมค่าว่างที่ขอบด้วยวิธี backward/forward fill เพื่อหลีกเลี่ยง NaN
    altitude_smoothed = altitude_smoothed.fillna(method="bfill").fillna(method="ffill")
    
    # ตรวจสอบว่ามีคอลัมน์ track หรือไม่ ถ้าไม่มีลองหา Direction
    if track_col in df_out.columns:
        track = pd.to_numeric(df_out[track_col], errors="coerce")
    elif "Direction" in df_out.columns:
        track = pd.to_numeric(df_out["Direction"], errors="coerce")
    else:
        # ถ้าไม่มี track หรือ Direction ให้ใช้ค่า NaN สำหรับการตรวจสอบ
        track = pd.Series([np.nan] * len(df_out))
    
    # ตรวจสอบว่ามีคอลัมน์ ROCD_m/s หรือไม่
    if rocd_col in df_out.columns:
        rocd = pd.to_numeric(df_out[rocd_col], errors="coerce")
    else:
        # ถ้าไม่มี ROCD_m/s ให้สร้างจากการคำนวณ altitude diff
        rocd = None

    # ในช่วงก่อนบิน (ground) เราจะยังไม่ตัดสินว่าเป็น Taxi_out หรือ Takeoff
    # ตัดสินใจช่วง Takeoff โดยย้อนจาก `Initial_climb` ในภายหลัง
    
    # เริ่มต้นด้วยค่า Unknown
    phases = pd.Series(["Unknown"] * len(df_out), dtype=object)
    
    # 1. หา Cruise Altitude (ปรับ: ใช้ค่าสูงสุด แล้วเลือกแถวที่อยู่ใกล้ค่าสูงสุด
    #    โดยไม่ต้องกำหนดว่าต้องซ้ำหลายแถว)
    cruise_altitude = _detect_cruise_altitude(altitude_smoothed)
    
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
    
    # 4. ใช้ state machine สำหรับ Climb/Cruise/Descent ที่ยืดหยุ่นได้
    # (จะกำหนด phases ผ่านฟังก์ชัน _assign_climb_cruise_descent_phases_v2 ด้านล่าง)
    cruise_start_idx = None
    cruise_end_idx = None
    first_cruise_idx = None
    
    # 5. จำแนก phases ด้วย state machine แบบใหม่
    phases = _assign_climb_cruise_descent_phases_v2(
        phases, altitude, altitude_smoothed, cruise_altitude, rocd=rocd
    )
    
    for i in range(len(df_out)):
        # ใช้ raw altitude สำหรับการตรวจสอบว่าเป็น ground
        alt_raw = altitude.iloc[i]
        # แต่ใช้ค่า smoothed สำหรับการตัดสิน phase (ป้องกัน short dip)
        alt = altitude_smoothed.iloc[i]
        is_ground = (alt_raw == 0) or (pd.isna(alt_raw)) or (alt_raw < 10)  # ถือว่าเป็น ground ถ้า < 10 ft

        # ถ้า state machine กำหนด phase สำหรับแถวนี้ (Climb/Cruise/Descent) ให้ข้ามไป
        if phases.iloc[i] in ["Climb", "Cruise", "Descent"]:
            continue
        
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
            # Flying phases (ที่ state machine ไม่ได้กำหนด)
            if alt > 0 and alt <= 2000:
                # Initial_climb: >0 ถึง 2000 ft
                if i > 0 and phases.iloc[i-1] in ["Approach", "Descent", "Landing"]:
                    # มาจากด้านบนเพื่อจะลงพื้น -> Landing
                    phases.iloc[i] = "Landing"
                else:
                    phases.iloc[i] = "Initial_climb"
            elif alt > 3000 and alt <= 8000:
                # Approach: มากกว่า 3000–8000 ft (เมื่อเป็นการลงจอดตามบริบท)
                if i > 0 and phases.iloc[i-1] in ["Descent", "Approach"]:
                    phases.iloc[i] = "Approach"
                else:
                    phases.iloc[i] = "Climb"
            elif alt > 8000:
                # Check ROCD to determine phase
                if rocd is not None and not pd.isna(rocd.iloc[i]):
                    rocd_val = rocd.iloc[i]
                    if rocd_val < -0.5:
                        # Significant descent
                        phases.iloc[i] = "Descent"
                    elif abs(rocd_val) <= 0.5:
                        # Check altitude stability for Cruise
                        window_size = 10
                        start_idx = max(0, i - window_size // 2)
                        end_idx = min(len(altitude_smoothed), i + window_size // 2 + 1)
                        alt_window = altitude_smoothed.iloc[start_idx:end_idx]
                        alt_range = alt_window.max() - alt_window.min() if len(alt_window) > 1 else 0
                        if alt_range <= 200:
                            phases.iloc[i] = "Cruise"
                        else:
                            phases.iloc[i] = "Climb"
                    else:
                        # ROCD > 0.5, climbing
                        phases.iloc[i] = "Climb"
                else:
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
    phases = _refine_flight_phases(phases, altitude, altitude_smoothed, cruise_altitude, cruise_start_idx, cruise_end_idx)
    
    # 8. ปรับปรุง Landing/Taxi_in ตาม Direction ที่ altitude = 0 ft
    phases = _refine_landing_taxi_in_by_direction(phases, altitude, track, tolerance_deg=5.0)
    
    df_out["flight_phase"] = phases
    return df_out


def _assign_climb_cruise_descent_phases_v2(
    phases: pd.Series,
    altitude: pd.Series,
    altitude_smoothed: pd.Series,
    cruise_altitude: Optional[float],
    rocd: Optional[pd.Series] = None,
    stability_window: int = 5,
    stability_threshold_ft: float = 50.0,
    min_descent_rows: int = 3,
) -> pd.Series:
    """
    State machine สำหรับ Climb/Cruise/Descent ที่ยืดหยุ่นได้
    
    Logic:
    1. Climb: altitude > 2000 ft และเพิ่มขึ้นต่อเนื่อง
    2. Cruise: altitude คงที่ (เสถียร) + ROCD_m/s ≈ 0 หลังจาก Climb
    3. Cruise → Climb: ถ้า altitude เพิ่มขึ้นต่อเนื่อง
    4. Cruise → Descent: ถ้า altitude ลดลงต่อเนื่อง
    5. Descent → Cruise: ถ้า altitude คงที่เสถียร + ROCD_m/s ≈ 0 ในช่วง Descent
    6. Climb ≠→ Descent: Climb ต้องผ่าน Cruise ก่อน
    
    States:
    0 = ground
    1 = climbing (altitude increasing)
    2 = cruise (altitude stable + ROCD ≈ 0)
    3 = descending (altitude decreasing)
    """
    refined = phases.copy()
    n = len(altitude_smoothed)
    
    if n == 0:
        return refined
    
    # คำนวณความเปลี่ยนแปลง altitude (diff)
    alt_diff = altitude_smoothed.diff()
    
    # State machine
    state = 0  # 0=ground, 1=climbing, 2=cruise, 3=descending
    stable_count = 0  # Counter for consecutive stable rows before entering Cruise
    
    for i in range(n):
        alt = altitude_smoothed.iloc[i]
        
        # ถ้า altitude <= 10 ให้ข้าม (ground phase)
        if pd.isna(alt) or alt <= 10:
            continue
        
        if state == 0:  # Ground/before_climb
            if alt > 2000:
                state = 1
                refined.iloc[i] = "Climb"
            elif alt > 0:
                refined.iloc[i] = "Initial_climb"
        
        elif state == 1:  # Climbing
            # ตรวจสอบ: altitude ลดลงอย่างชัดเจนหรือไม่? (ต้อง < -75 ft/row) → เปลี่ยน state เป็น Descent
            if i > 0 and alt_diff.iloc[i] < -75:
                # เริ่มลดลงอย่างชัดเจน -> Descent
                state = 3
                refined.iloc[i] = "Descent"
            # ตรวจสอบ: altitude เสถียร AND ROCD ≈ 0 -> Cruise
            # OR: altitude range ≤ 75 ft in recent rows (bumpy cruise detection)
            elif i > 0:
                # Check past 10 rows for altitude stability (bumpy cruise indicator)
                recent_range = min(10, i)  # Check more rows for stability
                past_altitudes = altitude_smoothed.iloc[i-recent_range+1:i+1]
                alt_range_past = past_altitudes.max() - past_altitudes.min() if len(past_altitudes) >= 2 else 0
                
                # Check ROCD (must be essentially 0, allow floating-point tolerance)
                rocd_value = rocd.iloc[i] if rocd is not None else None
                
                # Check if altitude is WELL ABOVE cruise level (alt >= cruise_alt + 30 ft)
                # This avoids marking rows as Cruise while still actively climbing TO cruise altitude
                # Row 1667 at 33075 will be Cruise (33075 >= 33038+30), but rows 1665-1666 stay Climb (33062 < 33068)
                at_cruise_level = cruise_altitude is not None and alt >= cruise_altitude + 30
                
                # Check rocd_value explicitly - if ROCD < -0.75, change to Descent
                if rocd_value is not None and rocd_value < -0.75:
                    # Aircraft is descending significantly - change to Descent
                    state = 3
                    refined.iloc[i] = "Descent"
                # PRIORITY RULE: If well above cruise altitude (alt >= cruise_alt + 30), mark as Cruise
                # This catches aircraft that have clearly reached cruise level
                elif at_cruise_level:
                    # Well above cruise altitude -> Cruise
                    state = 2
                    refined.iloc[i] = "Cruise"
                elif rocd_value is not None and rocd_value > 1.0:
                    # ROCD is strongly positive, don't enter Cruise yet (still climbing)
                    stable_count = 0
                    refined.iloc[i] = "Climb"
                elif alt_range_past <= 100.0 and rocd_value is not None and rocd_value > -0.1 and rocd_value < 0.1:
                    # Stable altitude range with VERY SMALL ROCD (±0.1) -> Cruise (not strong climb/descent)
                    state = 2
                    refined.iloc[i] = "Cruise"
                elif alt_range_past <= 1.0:
                    # ROCD is ~0 (between -0.001 and +0.001) and altitude very stable (<= 1 ft)
                    prev_alt = altitude_smoothed.iloc[i-1] if i > 0 else None
                    if prev_alt is not None and abs(alt - prev_alt) <= 1.0:
                        stable_count += 1
                    else:
                        stable_count = 1
                    if stable_count >= stability_window:
                        state = 2
                        refined.iloc[i] = "Cruise"
                    else:
                        refined.iloc[i] = "Climb"
                # NEW RULE: Catch level flight at any altitude
                # If ROCD is essentially 0 (very small positive or negative) AND altitude very stable AND high altitude
                # Then transition to Cruise (catches intermediate cruise levels like SL759 rows at 33000)
                elif rocd_value is not None and abs(rocd_value) <= 0.02 and alt_range_past <= 5.0 and alt > 8000:
                    # Level flight with essentially zero ROCD -> Cruise
                    state = 2
                    refined.iloc[i] = "Cruise"
                else:
                    stable_count = 0
                    refined.iloc[i] = "Climb"
            elif alt > 2000:
                # ไม่เพิ่มขึ้นและยังสูง -> ถือว่า Climb ต่อ
                refined.iloc[i] = "Climb"
            else:
                refined.iloc[i] = "Initial_climb"
        
        elif state == 2:  # Cruise (Stable altitude + ROCD ≈ 0)
            # Allow small altitude fluctuations while in Cruise (bumpy cruise)
            # Entry/Exit are strict, but staying in Cruise can tolerate ROCD variations
            recent_range = min(10, i)  # Check past 10 rows for stability
            past_altitudes = altitude_smoothed.iloc[i-recent_range+1:i+1]
            alt_range = past_altitudes.max() - past_altitudes.min() if len(past_altitudes) >= 2 else 0
            
            # Check ROCD - allow small variations for bumpy cruise (±0.5 m/s)
            rocd_value = rocd.iloc[i] if rocd is not None else None
            # Use explicit range check for stability (±0.5 m/s tolerance)
            rocd_is_stable = (rocd_value is not None) and (rocd_value > -0.5 and rocd_value < 0.5)
            
            # Check if altitude is at or near cruise level (with 150 ft tolerance below, 100 ft above)
            at_cruise_level = cruise_altitude is not None and alt >= cruise_altitude - 150 and alt <= cruise_altitude + 100
            
            # PRIMARY RULE: Check ROCD < -0.75 (significant descent - exit cruise)
            if rocd_value is not None and rocd_value < -0.75:
                # Aircraft is descending significantly - exit Cruise
                state = 3
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
            # CRITICAL RULE: If altitude already BELOW cruise AND descending → exit Cruise to Descent
            elif cruise_altitude is not None and alt < cruise_altitude and rocd_value is not None and rocd_value < -0.3:
                state = 3
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
            # Stay in Cruise if at cruise level and ROCD not too negative
            elif at_cruise_level and rocd_value is not None and rocd_value > -0.75:
                refined.iloc[i] = "Cruise"
            # Check for ROCD > 0.5 with significant altitude change
            elif rocd_value is not None and rocd_value > 0.5 and alt_range > 75.0:
                # Aircraft is climbing significantly -> exit Cruise to Climb
                state = 1
                refined.iloc[i] = "Climb"
            # ROCD is stable (±0.5) - check if can stay in Cruise
            elif rocd_is_stable and alt_range <= 50.0:
                # Perfect cruise - ROCD stable AND altitude very stable
                refined.iloc[i] = "Cruise"
            elif rocd_is_stable and alt_range <= 75.0 and alt > 8000:
                # Bumpy cruise - ROCD must be ~0 AND altitude within 75 ft
                refined.iloc[i] = "Cruise"
            else:
                # Exit Cruise in all other cases
                state = 1
                refined.iloc[i] = "Climb"
        
        elif state == 3:  # Descending (or in Approach/Landing)
            # MOST IMPORTANT: Check ROCD first
            # User mandate: "ดูแค่คอลัม ROCD_m/s และ altitude เท่านั้น"
            rocd_value = rocd.iloc[i] if rocd is not None else None
            
            # Check altitude stability in recent 2-row window
            # Use the same window semantics as other states (i-recent_range+1 : i+1)
            recent_range = min(2, i)
            start_idx = max(0, i - recent_range + 1)
            past_altitudes = altitude_smoothed.iloc[start_idx:i+1]
            alt_range = past_altitudes.max() - past_altitudes.min() if len(past_altitudes) >= 2 else float('inf')
            
            # Check if altitude is at cruise level (within 30 ft margin) - stricter check
            # This only catches genuine cruise plateau scenarios
            at_cruise_level = cruise_altitude is not None and alt >= cruise_altitude - 30 and alt <= cruise_altitude + 100
            
            # PRIMARY RULE: If at cruise level with VERY SMALL ROCD (±0.2) → Cruise plateau
            # CRITICAL: Don't mark as Cruise if we're clearly in continuous descent (altitude < cruise_alt AND ROCD negative)
            # But DO mark as Cruise if altitude is stable with ROCD≈0, even if not at the max detected cruise_altitude
            if (at_cruise_level and rocd_value is not None and rocd_value > -0.2 and rocd_value < 0.2):
                state = 2
                refined.iloc[i] = "Cruise"
            # SECONDARY RULE: Any ROCD < -0.5 means aircraft is descending significantly (stay in Descent/Approach/Landing)
            # Keep at -0.5 threshold here (this is about detecting DESCENT, so -0.5 is appropriate)
            elif rocd_value is not None and rocd_value < -0.5:
                # Aircraft is descending significantly - classify by altitude
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
            # NEW CATCH-ALL: If altitude is stable (ROCD ±0.02) AND altitude very stable (range ≤5ft) AND high altitude
            # Then mark as Cruise even if not at detected cruise_altitude (catches intermediate levels like SL759 at 33000)
            elif rocd_value is not None and abs(rocd_value) <= 0.02 and alt_range <= 5.0 and alt > 8000:
                # Level flight or near-level at high altitude -> Cruise
                state = 2
                refined.iloc[i] = "Cruise"
            # TERTIARY RULE: If ROCD between -0.1 and 0.1 m/s AND altitude stable AND not below cruise → Cruise (level flight)
            # CRITICAL: Tightened from ±0.5 to ±0.1 to prevent active climbing from being marked as Cruise
            elif (rocd_value is not None and rocd_value > -0.1 and rocd_value < 0.1 and alt_range <= 50 and alt > 8000 
                  and (cruise_altitude is None or alt > cruise_altitude)):
                state = 2
                refined.iloc[i] = "Cruise"
            # Quaternary: Altitude stable but altitude range shows some movement, classify by altitude range
            elif alt_range > 150:
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
            else:
                # Ambiguous - stay in current descent phase
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
    
    return refined


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
    
    # หาแถวแรกที่ altitude = 0 ft หลังบินลง (CRITICAL: ต้องไม่ใช่ NaN)
    first_zero_after_flight_idx = None
    for i in range(last_flying_idx + 1, len(altitude)):
        alt = altitude.iloc[i]
        # ต้องมีค่า (ไม่ใช่ NaN) และ altitude = 0 เท่านั้น
        if not pd.isna(alt) and (alt == 0 or alt < 10):
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
        if prev_alt <= 3000 and prev_alt > 0:
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
    # CRITICAL: ต้องไม่มี NaN
    ground_indices = []
    for i in range(first_zero_after_flight_idx, len(altitude)):
        alt = altitude.iloc[i]
        # ต้องมีค่า (ไม่ใช่ NaN) และ altitude = 0 เท่านั้น
        is_ground = (not pd.isna(alt)) and (alt == 0 or alt < 10)
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


def _refine_flight_phases(phases: pd.Series, altitude: pd.Series, altitude_smoothed: pd.Series, cruise_altitude: Optional[float], cruise_start_idx: Optional[int] = None, cruise_end_idx: Optional[int] = None) -> pd.Series:
    """
    Refine flight phases - add minimal Landing detection
    
    Key rules:
    - Landing: altitude 0-3000 ft after Descent/Approach, or between Approach/Descent and Taxi_in
    - Prevent Initial_climb after Approach/Descent (must come after Taxi_out/Takeoff)
    - Do NOT promote Climb to Cruise
    """
    refined = phases.copy()
    
    # Simple Landing detection: altitude 0-3000 ft after Descent/Approach
    for i in range(1, len(refined)):
        alt = altitude.iloc[i]
        
        # If altitude <= 3000 and > 0, check if it should be Landing
        if alt > 0 and alt <= 3000:
            if i > 0:
                prev_phase = refined.iloc[i-1]
                if prev_phase in ["Descent", "Approach", "Landing"]:
                    # Coming from descent/approach -> Landing
                    refined.iloc[i] = "Landing"
        
        # Prevent Initial_climb after Descent/Approach
        if refined.iloc[i] == "Initial_climb":
            if i > 0:
                prev_phase = refined.iloc[i-1]
                if prev_phase in ["Descent", "Approach", "Landing"]:
                    # After descent/approach, should be Landing, not Initial_climb
                    refined.iloc[i] = "Landing"
    
    return refined

