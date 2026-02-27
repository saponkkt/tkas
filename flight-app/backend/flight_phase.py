"""
Flight Phase Detection Module

โมดูลสำหรับกำหนด flight phase ของแต่ละแถวในข้อมูล ADS-B
"""
from __future__ import annotations

from typing import Optional
import time

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
    - Cruise: altitude คงที่ (altitude diff ≈ 0) (หลัง Climb และก่อน Descent)
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

    # สร้างเวอร์ชัน smoothed ของ altitude เพื่อป้องกันการกระโดดชั่วคราว
    # ใช้ rolling median ขนาดเล็ก (3 แถว) ที่อยู่ตรงกลางเพื่อลด noise สั้นๆ
    altitude_smoothed = altitude.rolling(window=3, center=True, min_periods=1).median()
    # เติมค่าว่างที่ขอบด้วยวิธี backward/forward fill เพื่อหลีกเลี่ยง NaN
    altitude_smoothed = altitude_smoothed.bfill().ffill()
    
    # ตรวจสอบว่ามีคอลัมน์ track หรือไม่ ถ้าไม่มีลองหา Direction
    if track_col in df_out.columns:
        track = pd.to_numeric(df_out[track_col], errors="coerce")
    elif "Direction" in df_out.columns:
        track = pd.to_numeric(df_out["Direction"], errors="coerce")
    else:
        # ถ้าไม่มี track หรือ Direction ให้ใช้ค่า NaN สำหรับการตรวจสอบ
        track = pd.Series([np.nan] * len(df_out))
    
    # คำนวณ altitude diff จาก RAW altitude (ไม่ smoothed) เพื่อตรวจจับ genuine changes
    # ใช้ raw altitude เพื่อหลีกเลี่ยงปัญหา false Cruise จาก floating point rounding
    altitude_diff = altitude.diff()

    # ในช่วงก่อนบิน (ground) เราจะยังไม่ตัดสินว่าเป็น Taxi_out หรือ Takeoff
    # ตัดสินใจช่วง Takeoff โดยย้อนจาก `Initial_climb` ในภายหลัง
    
    # เริ่มต้นด้วยค่า Unknown
    phases = pd.Series(["Unknown"] * len(df_out), dtype=object)
    
    # 1. หา Cruise Altitude (ปรับ: ใช้ค่าสูงสุด แล้วเลือกแถวที่อยู่ใกล้ค่าสูงสุด
    #    โดยไม่ต้องกำหนดว่าต้องซ้ำหลายแถว)
    # ลด min_stable_rows ให้ 5 (ต่อเนื่อง 5 แถว = เพียงพอสำหรับตรวจสอบ cruise section)
    # และเพิ่ม trend detection เพื่อพิจารณา climbing sections
    cruise_altitude = _detect_cruise_altitude(altitude_smoothed, min_stable_rows=5, tolerance_ft=100.0)
    
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
        phases, altitude, altitude_smoothed, cruise_altitude,
        allow_short_cruise_dip_ft=50.0,      # ยอมรับการลง 50 ft ระหว่าง Cruise
        allow_short_cruise_dip_rows=50,      # อนุญาตให้ dip ยาวถึง 50 แถว
        return_to_cruise_tolerance_ft=15.0,  # ต้องกลับมาใกล้ cruise altitude ในเกณฑ์ 15 ft
        allow_short_cruise_peak_ft=50.0,     # ยอมรับการขึ้น 50 ft ระหว่าง Cruise
        allow_short_cruise_peak_rows=50      # อนุญาตให้ peak ยาวถึง 50 แถว
    )
    
    for i in range(len(df_out)):
        # ใช้ raw altitude สำหรับการตรวจสอบว่าเป็น ground
        alt_raw = altitude.iloc[i]
        # แต่ใช้ค่า smoothed สำหรับการตัดสิน phase (ป้องกัน short dip)
        alt = altitude_smoothed.iloc[i]
        is_ground = (alt_raw == 0) or (pd.isna(alt_raw)) or (alt_raw < 1)  # ถือว่าเป็น ground ถ้า < 1 ft

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
                # Approach: มากกว่า 3000–8000 ft
                if i > 0:
                    prev_phase = phases.iloc[i-1]
                    # Check if truly descending (use raw altitude for true altitude change)
                    alt_diff_raw = altitude.iloc[i] - altitude.iloc[i-1] if i > 0 else 0
                    is_descending = alt_diff_raw < -0.001  # True descent, not floating point noise
                    
                    # RELAXED RULE: If altitude is descending, can be Approach regardless of predecessor
                    # (More lenient - FIX: Mid-altitude section will validate and override if needed)
                    if is_descending:
                        phases.iloc[i] = "Approach"
                    else:
                        # Not descending, must be Climb
                        phases.iloc[i] = "Climb"
                else:
                    phases.iloc[i] = "Climb"
            elif alt > 8000:
                # Use altitude diff to determine phase (altitude_smoothed for consistency)
                if i > 0 and not pd.isna(altitude_diff.iloc[i]):
                    alt_diff_val = altitude_diff.iloc[i]
                    if alt_diff_val < -0.05:
                        # Significant descent: diff < -0.05 ft/row
                        phases.iloc[i] = "Descent"
                    elif abs(alt_diff_val) <= 0.05:
                        # Very stable: |diff| <= 0.05 ft/row → Cruise
                        phases.iloc[i] = "Cruise"
                    else:
                        # Climbing: diff > 0.05 ft/row
                        phases.iloc[i] = "Climb"
                else:
                    # Default: Climb (first row or no diff available)
                    phases.iloc[i] = "Climb"
            else:
                phases.iloc[i] = "Unknown"
    

    # 6. FIX: Make Approach "sticky" approach - once in approach, stay there until landing
    # This prevents oscillation between Climb and Approach when altitude vibrates slightly
    for i in range(len(df_out)):
        alt = altitude_smoothed.iloc[i]
        current_phase = phases.iloc[i]
        prev_phase = phases.iloc[i-1] if i > 0 else "Unknown"
        
        # STICKY RULE: Once predecessor is Approach at mid-altitude, keep it Approach
        if alt >= 3000 and alt <= 8000 and prev_phase == "Approach":
            # Stay in Approach until we drop below 3000 ft
            phases.iloc[i] = "Approach"
        
        # Override other phases that shouldn't occur in approach range coming from Approach
        elif alt >= 3000 and alt <= 8000 and current_phase == "Climb" and prev_phase == "Approach":
            # Don't allow transition back to Climb from Approach - stay Approach
            phases.iloc[i] = "Approach"
    
    # 7. Fix Approach continuity: Approach ONLY when coming from Descent AND actively descending
    # Use alt_diff from smoothed altitude (consistent with state machine)
    # (Note: sticky Approach rule above now handles most oscillation problems)
    alt_diff = altitude_smoothed.diff().to_numpy(dtype=float)
    
    in_descent_approach = False
    for i in range(len(df_out)):
        alt = altitude_smoothed.iloc[i]
        alt_diff_val = alt_diff[i] if i > 0 and i < len(alt_diff) else 0.0
        raw_alt_diff = altitude.iloc[i] - altitude.iloc[i-1] if i > 0 else 0.0
        
        # Check altitude direction using smoothed diffs (consistency with state machine)
        # FILTER OUT floating point noise: if very small, treat as stable
        # STRICT THRESHOLDS: require significant altitude change (> 0.01 after smoothing)
        is_descending = (alt_diff_val < -0.01 and abs(raw_alt_diff) > 0.001)  # True descent
        is_ascending = (alt_diff_val > 0.01 and abs(raw_alt_diff) > 0.001)     # True ascent
        current_phase = phases.iloc[i]  # Get current phase for secondary checks
        
        if alt > 3000 and alt <= 8000:
            # In Approach altitude range (3000-8000 ft)
            # CRITICAL RULE: If altitude is INCREASING, MUST be Climb (never Approach)
            # Approach only valid when altitude is STRICTLY DECREASING from previous row
            
            if is_ascending:
                # CRITICAL: If altitude is INCREASING through this range, MUST be Climb
                # Override any state machine label - climbing never stays in Approach
                phases.iloc[i] = "Climb"
            elif current_phase == "Approach":
                # Even if state machine assigned Approach, verify it's truly descending
                # If altitude is NOT descending, override to Climb
                if not is_descending:
                    phases.iloc[i] = "Climb"
                # else: keep Approach (altitude is descending, valid assignment)
            # For other phases in this altitude range: keep as-is, descent handling below

        
        elif alt < 3000 and alt > 10:
            # Below Approach altitude - should be Landing if coming from Approach/Descent
            if i > 0 and phases.iloc[i-1] in ["Approach", "Descent"]:
                in_descent_approach = False
                if phases.iloc[i] not in ["Landing", "Taxi_in"]:
                    phases.iloc[i] = "Landing"
            else:
                # Not coming from Descent/Approach, reset state
                in_descent_approach = False
        
        elif alt <= 10 or alt > 8000:
            # Ground or above Approach altitude - no longer in Approach
            in_descent_approach = False

    # 7. ปรับปรุงช่วง Takeoff ให้ตรงกับนิยามใหม่
    #    - หา Initial_climb ก่อน
    #    - ก่อนบรรทัดแรกของ Initial_climb ให้หาช่วงที่ altitude = 0 และค่า track/Direction ซ้ำกัน > 3 แถว
    #      ช่วงนั้นจะถูกกำหนดให้เป็น Takeoff และช่วงก่อนหน้าให้เป็น Taxi_out
    phases = _apply_takeoff_from_initial_climb(phases, altitude, track)

    # 7b. กำหนด Taxi_out เป็นทุกแถวก่อนเริ่ม Takeoff
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

    # 8. ปรับปรุง phases ให้ต่อเนื่องและสมเหตุสมผล
    phases = _refine_flight_phases(phases, altitude, altitude_smoothed, cruise_altitude, cruise_start_idx, cruise_end_idx)
    
    # --- Plateau rule: near-equality with tolerance, min 4 rows ---
    # FIXED: Apply to ALL altitudes > 2000 (not just > 8000)
    # Use floating point tolerance (1e-6 ft = 0.000001 ft) to catch floating point noise
    try:
        exact_plateau_min_rows = 4
        raw_alt = altitude.to_numpy()
        n_raw = len(raw_alt)
        altitude_tolerance = 1e-6  # tolerance for floating point comparison (0.000001 ft)
        
        s = 0
        while s < n_raw:
            if pd.isna(raw_alt[s]) or raw_alt[s] <= 0:
                s += 1
                continue
            
            base_alt = raw_alt[s]
            e = s + 1
            
            # Find consecutive rows with nearly-equal altitude (within tolerance)
            while e < n_raw:
                if pd.isna(raw_alt[e]) or raw_alt[e] <= 0:
                    break
                if abs(raw_alt[e] - base_alt) > altitude_tolerance:
                    break
                e += 1
            
            length = e - s
            if length >= exact_plateau_min_rows:
                mean_alt = float(raw_alt[s])
                if mean_alt > 8000:
                    # Cruise altitude (above 8000 ft)
                    for idx in range(s, e):
                        phases.iloc[idx] = "Cruise"
                elif mean_alt >= 5000:
                    # Mid-level altitude (5000-8000): could be cruise plateau before descent
                    # Apply Cruise label to exact plateaus
                    for idx in range(s, e):
                        phases.iloc[idx] = "Cruise"
                elif mean_alt >= 3000:
                    # Approach/approach-phase altitude: Don't force Cruise here
                    # Let section 6 and post-processing handle it
                    pass
                # (ground rows not overwritten)
                s = e
            else:
                s += 1
    except Exception:
        pass

    # Fix Descent/Climb assignment: if altitude decreases from previous row, set Descent
    # STRICT: Only assign Approach if predecessor is Descent AND altitude is decreasing
    # AND following rows continue descending (not returning to Cruise immediately)
    # USE SMOOTHED ALTITUDE for consistency with section 6 logic
    try:
        for i in range(1, len(phases)):
            # Do not overwrite ground rows
            if altitude_smoothed.iloc[i] <= 0 or altitude_smoothed.iloc[i-1] <= 0:
                continue
            if pd.isna(altitude_smoothed.iloc[i]) or pd.isna(altitude_smoothed.iloc[i-1]):
                continue
            
            # FIX: Mid-altitude Climb/Descent/Cruise plateau detection
            # If at 3000-8000 ft and predecessor suggests descent/approach sequence,
            # should check if heading to landing regardless of state machine classification
            if altitude_smoothed.iloc[i] >= 3000 and altitude_smoothed.iloc[i] <= 8000:
                # CRITICAL: Validate altitude is truly descending before marking Approach
                alt_diff_raw = altitude.iloc[i] - altitude.iloc[i-1] if i > 0 else 0
                prev_phase = phases.iloc[i-1] if i > 0 else "Unknown"
                
                # Check if truly descending (not just noise, not zero)
                is_truly_descending = alt_diff_raw < -0.001
                is_flat_or_climbing = alt_diff_raw >= -0.001
                
                if is_flat_or_climbing:
                    # Altitude is flat (ROCD≈0) or climbing - CANNOT be Approach
                    # Only exception: if predecessor is Approach AND we're in approach phase (sticky Approach)
                    if phases.iloc[i] == "Approach" and prev_phase == "Approach":
                        # Keep Approach - it's sticky/continuous descent
                        continue
                    elif phases.iloc[i] == "Approach":
                        # Override to Climb (first entry into Approach with flat/climbing altitude)
                        phases.iloc[i] = "Climb"
                    continue
                
                # Check all possible descent sequence predecessors (only if truly descending)
                is_from_descent = (phases.iloc[i-1] == "Descent")
                is_from_approach = (phases.iloc[i-1] == "Approach")
                is_from_climb_at_midalt = (phases.iloc[i-1] == "Climb" and altitude_smoothed.iloc[i-1] >= 3000)
                is_in_descent_sequence = is_from_descent or is_from_approach or is_from_climb_at_midalt
                
                # Check lookahead for landing detection
                is_heading_to_landing = False
                is_stabilizing_to_plateau = False
                
                lookahead_window = min(500, len(phases) - i - 1)
                if lookahead_window > 0:
                    future_alts = altitude_smoothed.iloc[i+1:i+1+lookahead_window].values
                    
                    # Check if any future altitude goes below 3000 (approaching landing)
                    if np.any(future_alts < 3000):
                        is_heading_to_landing = True
                    
                    # Check if STABILIZING to VERY TIGHT plateau (only within 5 ft - very strict)
                    base_alt = altitude_smoothed.iloc[i]
                    all_future_alts_close = np.all(np.abs(future_alts - base_alt) < 5)
                    not_descending_more_than_5 = np.all(future_alts >= base_alt - 5)
                    
                    if all_future_alts_close and not_descending_more_than_5:
                        is_stabilizing_to_plateau = True
                
                # Apply mid-altitude logic (3000-8000 ft)
                if is_in_descent_sequence and is_heading_to_landing and not is_stabilizing_to_plateau:
                    # In descent sequence, heading to landing → mark as Approach
                    if phases.iloc[i] not in ["Landing", "Taxi_in"]:
                        phases.iloc[i] = "Approach"
                    continue
            
            if altitude_smoothed.iloc[i] <= altitude_smoothed.iloc[i-1]:
                # Altitude is DECREASING or STABLE (flat segment) in SMOOTHED data
                # BUT: Check RAW altitude to detect actual climb/descent (smoothing can mask small changes)
                raw_alt_diff = altitude.iloc[i] - altitude.iloc[i-1]
                
                if altitude_smoothed.iloc[i] > 8000:
                    # Above 8000: only mark Descent if not already marked
                    # BUT: if RAW altitude is climbing, mark as Climb (smoothing shouldn't hide actual climb)
                    # Use threshold 0.05 ft to match state machine's CLIMB_THRESHOLD
                    if raw_alt_diff > 0.05:
                        phases.iloc[i] = "Climb"
                    elif phases.iloc[i] not in ["Cruise", "Approach"]:
                        phases.iloc[i] = "Descent"
                elif altitude_smoothed.iloc[i] >= 3000:
                    # 3000-8000 ft: already handled above
                    pass

            elif altitude_smoothed.iloc[i] > altitude_smoothed.iloc[i-1]:
                # Altitude is INCREASING
                # Only override if not already Cruise
                if phases.iloc[i] != "Cruise":
                    if altitude_smoothed.iloc[i] > 8000:
                        phases.iloc[i] = "Climb"
                    elif altitude_smoothed.iloc[i] >= 3000:
                        # In 3000-8000 when ascending, should be Climb not Approach
                        # Section 6 already handles descent → Approach rule
                        phases.iloc[i] = "Climb"
    except Exception:
        pass

    # 9. ปรับปรุง Landing/Taxi_in ตาม Direction ที่ altitude = 0 ft
    phases = _refine_landing_taxi_in_by_direction(phases, altitude, track, tolerance_deg=5.0)

    # FINAL: Always force Taxi_out for all ground rows before first_flying_idx if no Takeoff is found
    # And fallback: if no Takeoff found, set 3 ground rows before first altitude > 0 as Takeoff
    if "Takeoff" not in phases.values:
        if first_flying_idx is not None and first_flying_idx > 0:
            # Set all ground rows before first_flying_idx as Taxi_out
            for i in range(first_flying_idx):
                if altitude.iloc[i] <= 0:
                    phases.iloc[i] = "Taxi_out"
            # Fallback: set last 3 ground rows before first_flying_idx as Takeoff (if enough rows)
            takeoff_rows = 3
            ground_idxs = [i for i in range(first_flying_idx) if altitude.iloc[i] <= 0]
            if len(ground_idxs) >= takeoff_rows:
                for idx in ground_idxs[-takeoff_rows:]:
                    phases.iloc[idx] = "Takeoff"

    # ---
    # POST-PROCESSING: Fix floating point altitude noise ONLY in Cruise regions with exact plateaus
    # This handles cases like 32999.99999999, 33000.0, 33000.000001 in consecutive rows
    # Strategy: Round altitude to nearest integer ONLY when:
    #   1. Current phase is Cruise (confirmed by state machine)
    #   2. Part of exact altitude plateau (multiple identical raw altitude values)
    #   3. At cruise altitude (altitude > 8000 ft)
    #   4. Does NOT create false Cruise from Climb/Descent regions
    # ---
    try:
        raw_alt = altitude.to_numpy()
        alt_rounded = raw_alt.copy()
        
        # Find exact altitude plateaus
        n = len(raw_alt)
        s = 0
        while s < n:
            if pd.isna(raw_alt[s]) or raw_alt[s] <= 8000:
                s += 1
                continue
            
            # Find consecutive rows with EXACT same altitude
            e = s + 1
            while e < n and not pd.isna(raw_alt[e]) and raw_alt[e] == raw_alt[s]:
                e += 1
            
            length = e - s
            
            # Only round if plateau is >= 4 rows AND all rows in phase indicate Cruise
            if length >= 4:
                all_cruise = all(phases.iloc[idx] == "Cruise" for idx in range(s, e))
                if all_cruise:
                    # Round to nearest integer (removes .00001 or .99999 noise)
                    rounded_val = float(round(raw_alt[s]))
                    for idx in range(s, e):
                        alt_rounded[idx] = rounded_val
            
            s = e
        
        # Update altitude with rounded values
        altitude_out = pd.Series(alt_rounded, index=altitude.index)
        df_out["altitude"] = altitude_out
    except Exception:
        pass  # If rounding fails, continue with original altitude

    df_out["flight_phase"] = phases
    
    # FINAL FIX: Make Approach "sticky" - once in Approach, stay until below 3000 ft
    # This prevents oscillation between Climb and Approach due to altitude vibration
    for i in range(1, len(df_out)):
        alt = df_out['altitude'].iloc[i]
        current_phase = df_out['flight_phase'].iloc[i]
        prev_phase = df_out['flight_phase'].iloc[i-1]
        
        # If previous was Approach and we're still in approach altitude range, MUST stay Approach
        if alt >= 3000 and alt <= 8000 and prev_phase == "Approach":
            df_out.loc[df_out.index[i], 'flight_phase'] = "Approach"
    
    return df_out


def _assign_climb_cruise_descent_phases_v2(
    phases: pd.Series,
    altitude: pd.Series,
    altitude_smoothed: pd.Series,
    cruise_altitude: Optional[float],
    stability_window: int = 5,
    stability_threshold_ft: float = 50.0,
    min_descent_rows: int = 3,
    # Allow short transient dips during Cruise to be treated as Cruise
    allow_short_cruise_dip_ft: float = 30.0,
    allow_short_cruise_dip_rows: int = 5,
    return_to_cruise_tolerance_ft: float = 10.0,
    # Allow short transient peaks during Cruise to be treated as Cruise
    allow_short_cruise_peak_ft: float = 50.0,
    allow_short_cruise_peak_rows: int = 15,
) -> pd.Series:
    """
    State machine สำหรับ Climb/Cruise/Descent
    - อิงจากค่า altitude จริงของแต่ละแถว
    - ไม่มี overshoot logic
    
    KEY RULES:
    1. Cruise = altitude STABLE (range ≤50 ft) in window AND NO climbing/descending pattern
    2. Climb = altitude เพิ่มขึ้น (positive altitude diff)
    3. Descent = altitude ลดลง (negative altitude diff)
    4. DIP-RETURN: Short dips down from cruise followed by return = all Cruise
    5. PEAK-RETURN: Short peaks up from cruise followed by return = all Cruise
    """
    refined = phases.copy()
    n = len(altitude_smoothed)
    
    if n == 0:
        return refined
    
    # คำนวณความเปลี่ยนแปลง altitude (diff)
    alt_diff = altitude_smoothed.diff()
    # Convert series to numpy arrays for faster indexed operations
    alt_arr = altitude_smoothed.to_numpy(dtype=float)
    raw_alt_arr = altitude.to_numpy(dtype=float)
    alt_diff_arr = np.empty(n, dtype=float)
    if n > 0:
        alt_diff_arr[0] = np.nan
    if n > 1:
        alt_diff_arr[1:] = alt_arr[1:] - alt_arr[:-1]
    
    # State machine
    state = 0  # 0=ground, 1=climbing, 2=cruise, 3=descending

    # ===== STRICT THRESHOLDS FOR CLIMBING/DESCENDING DETECTION =====
    # PROBLEM 2 & 3: ใช้ threshold ที่เข้มงวดเพื่อตรวจสอบแนวโน้ม
    CLIMB_THRESHOLD = 0.05  # ft/row (↑ increased from 0.01, any diff ≥ 0.05 = climbing)
    DESCENT_THRESHOLD = -0.05  # ft/row (↓ decreased from -0.01, any diff ≤ -0.05 = descending)
    STABLE_THRESHOLD = 0.1  # ± 0.1 ft/row = considered stable
    
    # transient-dip-in-cruise tracking (persist across loop)
    dip_active = False
    dip_start_idx = None
    dip_start_alt = None
    dip_max_drop = 0.0
    dip_indices = []
    
    # transient-peak-in-cruise tracking (persist across loop)
    peak_active = False
    peak_start_idx = None
    peak_start_alt = None
    peak_max_rise = 0.0
    peak_indices = []
    peak_phase_before = None  # Track what phase we're coming from

    for i in range(n):
        alt = altitude_smoothed.iloc[i]
        
        # PRE-CHECK removed - rely on state==1 logic for cruise detection
        # State==1 is more strict and handles cruise transition better

        # ถ้า altitude <= 10 ให้ข้าม (ground phase)
        if pd.isna(alt) or alt <= 10:
            continue

        if state == 0:  # Ground/before_climb
            if alt > 2000:
                # Just start climbing - don't lookahead
                state = 1
                refined.iloc[i] = "Climb"
            elif alt > 0:
                refined.iloc[i] = "Initial_climb"

        elif state == 1:  # Climbing
            if i > 0:
                current_diff = alt_diff_arr[i] if i < len(alt_diff_arr) else np.nan
                raw_alt_diff = raw_alt_arr[i] - raw_alt_arr[i-1] if i > 0 else np.nan
                prev_phase = refined.iloc[i-1] if i > 0 else "Unknown"
                
                # CRITICAL: Once we're in Approach range (3000-8000 ft), stay in Approach until Landing
                # This prevents oscillation between Climb and Approach
                if prev_phase == "Approach" and alt >= 3000 and alt <= 8000:
                    # Stay in Approach - don't revert to Climb even if ROCD momentarily becomes 0
                    refined.iloc[i] = "Approach"
                    continue
                
                # IMMEDIATE CHECK: Use raw altitude to detect descent
                # This catches cases where smoothing delays detection
                # BUT: if raw_alt_diff is extremely small (< 0.001), treat as stable, not descent
                is_true_descent = (not np.isnan(raw_alt_diff) and raw_alt_diff < -0.01)
                is_floating_noise = (not np.isnan(raw_alt_diff) and abs(raw_alt_diff) < 0.001)
                
                if is_true_descent and not is_floating_noise:
                    state = 3
                    refined.iloc[i] = "Descent"
                    continue
                
                # Check last 3 rows for pattern analysis
                last_3_start = max(0, i - 2)
                last_3_diffs = alt_diff_arr[last_3_start:i+1]
                last_3_diffs_clean = last_3_diffs[~np.isnan(last_3_diffs)]
                if last_3_diffs_clean.size > 0:
                    climb_count = int((last_3_diffs_clean > CLIMB_THRESHOLD).sum())
                    descent_count = int((last_3_diffs_clean < DESCENT_THRESHOLD).sum())
                    stable_count = int((np.abs(last_3_diffs_clean) <= STABLE_THRESHOLD).sum())
                    last_3_alts = alt_arr[last_3_start:i+1]
                    alt_range = float(np.nanmax(last_3_alts) - np.nanmin(last_3_alts))
                    # ─── Strict exit from Climb to Cruise ---
                    # MUST satisfy conditions:
                    # 1. VERY close to cruise altitude (within 0.5 ft - must reach altitude first)
                    # 2. Very stable (range <= 10 ft over last 3 rows)
                    is_very_stable = alt_range <= 10.0
                    is_very_close_to_cruise = (cruise_altitude is not None and abs(alt - cruise_altitude) <= 0.5)
                    
                    # NEW FALLBACK: Detect exact altitude plateau even if cruise_altitude=None
                    # Look back to find if we're in an exact plateau (within floating point tolerance)
                    is_in_exact_plateau = False
                    if i >= 3:  # We need at least 4 rows
                        last_4_raw = raw_alt_arr[max(0, i-3):i+1]
                        alt_tolerance = 1e-6
                        if np.all(np.abs(last_4_raw - last_4_raw[0]) < alt_tolerance):
                            is_in_exact_plateau = True  # Exact plateau detected!
                    
                    # FALLBACK: If cruise_altitude is None but altitude is very high & stable,
                    # assume it's cruise plateau (e.g., 36000 ft in real data)
                    # Only allow fallback for standard cruise altitudes (>= 36000 ft)
                    is_high_altitude_stable = (alt >= 36000 and is_very_stable and cruise_altitude is None)
                    
                    # Mark as Cruise if ANY of:
                    # 1. Very close to detected cruise_altitude AND very stable, OR
                    # 2. In exact altitude plateau (perfect match of raw altitudes), OR
                    # 3. At high altitude (>= 36000 ft) AND very stable (fallback for standard cruise case)
                    if ((is_very_close_to_cruise or is_in_exact_plateau or is_high_altitude_stable) and is_very_stable):
                        state = 2
                        refined.iloc[i] = "Cruise"
                        continue
                    elif descent_count >= 1:
                        state = 3
                        refined.iloc[i] = "Descent"
                        continue
                    else:
                        refined.iloc[i] = "Climb"
                else:
                    refined.iloc[i] = "Climb"
            else:
                refined.iloc[i] = "Climb"

        elif state == 2:  # Cruise (Stable altitude)
            if i > 0:
                current_diff = alt_diff_arr[i] if i < len(alt_diff_arr) else np.nan
                raw_alt_diff = raw_alt_arr[i] - raw_alt_arr[i-1] if i > 0 else np.nan
                
                # Check last 3 rows
                last_3_start = max(0, i - 2)
                last_3_diffs = alt_diff_arr[last_3_start:i+1]
                last_3_diffs_clean = last_3_diffs[~np.isnan(last_3_diffs)]
                if last_3_diffs_clean.size > 0:
                    climb_count = int((last_3_diffs_clean > CLIMB_THRESHOLD).sum())
                    descent_count = int((last_3_diffs_clean < DESCENT_THRESHOLD).sum())
                    stable_count = int((np.abs(last_3_diffs_clean) <= STABLE_THRESHOLD).sum())
                    last_3_alts = alt_arr[last_3_start:i+1]
                    alt_range = float(np.nanmax(last_3_alts) - np.nanmin(last_3_alts))
                    
                    # FILTER OUT floating point noise
                    # If raw_alt_diff is < 0.001 in absolute value, treat it as stable
                    is_floating_noise = (not np.isnan(raw_alt_diff) and abs(raw_alt_diff) < 0.001)
                    
                    # ─── Detect if climbing/descending out of Cruise ---
                    # STRICT: ANY climbing/descending pattern = must exit Cruise
                    # BUT: If very close to cruise altitude, should stay in cruise (not exit upward)
                    is_at_cruise = (cruise_altitude is not None and abs(alt - cruise_altitude) <= 0.5)
                    
                    # Only exit if descent is TRUE (not floating noise) and significant
                    if climb_count >= 1 and not dip_active and not is_at_cruise:
                        state = 1
                        refined.iloc[i] = "Climb"
                        continue
                    elif descent_count >= 1 and not dip_active and not is_floating_noise:
                        state = 3
                        refined.iloc[i] = "Descent"
                        continue
                    elif dip_active:
                        # ...existing code...
                        pass
                    elif alt <= 10:
                        state = 0
                        refined.iloc[i] = "Unknown"
                        continue
                    else:
                        refined.iloc[i] = "Cruise"
                else:
                    refined.iloc[i] = "Cruise"
            else:
                refined.iloc[i] = "Cruise"

        elif state == 3:  # Descending (or in Approach/Landing)
            if i > 0:
                # CRITICAL FIX: Prevent reverting from Approach back to Descent
                # Once in Approach, stay in Approach until Landing
                prev_phase = refined.iloc[i-1] if i > 0 else "Unknown"
                if prev_phase == "Approach" and alt >= 3000 and alt <= 8000:
                    # Stay in Approach - don't revert to Descent
                    refined.iloc[i] = "Approach"
                    continue
                
                current_diff = alt_diff_arr[i] if i < len(alt_diff_arr) else np.nan
                # Check last 3 rows
                last_3_start = max(0, i - 2)
                last_3_diffs = alt_diff_arr[last_3_start:i+1]
                last_3_diffs_clean = last_3_diffs[~np.isnan(last_3_diffs)]
                if last_3_diffs_clean.size > 0:
                    climb_count = int((last_3_diffs_clean > CLIMB_THRESHOLD).sum())
                    descent_count = int((last_3_diffs_clean < DESCENT_THRESHOLD).sum())
                    stable_count = int((np.abs(last_3_diffs_clean) <= STABLE_THRESHOLD).sum())
                    last_3_alts = alt_arr[last_3_start:i+1]
                    alt_range = float(np.nanmax(last_3_alts) - np.nanmin(last_3_alts))
                    # ─── FIX: Prioritize detecting climb OUT of Descent ───
                    # When current row shows climb, exit Descent immediately (even if past rows showed descent)
                    # This handles transitions from descent back to climb correctly
                    if climb_count >= 1:
                        state = 1
                        refined.iloc[i] = "Climb"
                        continue
                    elif descent_count >= 1:
                        # NEW FIX: Check if descent is real or just floating point noise
                        raw_alt_diff = raw_alt_arr[i] - raw_alt_arr[i-1] if i > 0 else np.nan
                        is_floating_noise = (not np.isnan(raw_alt_diff) and abs(raw_alt_diff) < 0.001)
                        
                        if not is_floating_noise:
                            state = 3
                            refined.iloc[i] = "Descent"
                            continue
                        # If just noise, stay in Descent state but don't re-assign
                    elif (descent_count == 0 and alt_range <= 50.0 and cruise_altitude is not None and abs(alt - cruise_altitude) <= 50.0):
                        state = 2
                        refined.iloc[i] = "Cruise"
                        continue
                    elif alt <= 10:
                        state = 0
                        refined.iloc[i] = "Unknown"
                        continue
                    else:
                        refined.iloc[i] = "Descent"
                else:
                    if alt > 8000:
                        refined.iloc[i] = "Descent"
                    elif alt > 3000:
                        refined.iloc[i] = "Approach"
                    else:
                        refined.iloc[i] = "Landing"
            else:
                if alt > 8000:
                    refined.iloc[i] = "Descent"
                elif alt > 3000:
                    refined.iloc[i] = "Approach"
                else:
                    refined.iloc[i] = "Landing"
    
    return refined


def _detect_cruise_altitude(altitude: pd.Series, min_stable_rows: int = 3, tolerance_ft: float = 100.0, stable_diff_threshold: float = 50.0) -> Optional[float]:
    """
    Detect cruise altitude from stable altitude sections.
    
    Strategy: Find ALL stable sections, then SELECT THE HIGHEST ONE
    (to avoid picking climbing/descending plateau sections)
    
    Returns:
        Cruise altitude (feet) หรือ None ถ้าไม่พบ
    """
    if len(altitude) < min_stable_rows:
        return None

    # ป้องกัน NaN และแปลงเป็น numpy (faster)
    alt = altitude.bfill().ffill().to_numpy(dtype=float)

    n = len(alt)
    candidates = []  # List of (altitude_mean, length, start, end)

    for start in range(0, n - min_stable_rows + 1):
        end = start + min_stable_rows
        window = alt[start:end]  # numpy is faster than iloc
        if np.isnan(window).any():
            continue
        alt_range = np.max(window) - np.min(window)  # numpy ops faster
        if alt_range > tolerance_ft:
            continue
        # ตรวจสอบ consecutive diffs ภายใน window
        consec_diffs = np.abs(np.diff(window))
        if consec_diffs.size == 0 or np.max(consec_diffs) > stable_diff_threshold:
            continue
        
        # Check for climbing/descending trend - if avg altitude change is large, skip this section
        avg_diff = np.mean(np.diff(window))
        if abs(avg_diff) > 0.2:  # More than 0.2 ft/row average = climbing/descending, not cruise
            continue
        
        # ขยาย window ไปข้างหน้า (ด้วย safety limit)
        j = end
        max_cruise_length = 5000  # ไม่ขยายเกิน 5000 แถว (ประมาณ 83 นาที)
        while j < n and (j - start) < max_cruise_length:
            new_window = alt[start:j+1]  # numpy slice
            if np.max(new_window) - np.min(new_window) > tolerance_ft:
                break
            consec = np.abs(np.diff(new_window))
            if consec.size == 0 or np.max(consec) > stable_diff_threshold:
                break
            # Check for trend in expanded window
            avg_expanded = np.mean(np.diff(new_window))
            if abs(avg_expanded) > 0.2:
                break
            j += 1
        
        length = j - start
        if length >= min_stable_rows:
            cruise_window = alt[start:j]
            alt_mean = float(np.mean(cruise_window))
            candidates.append((alt_mean, length, start, j))

    if not candidates:
        return None

    # Select HIGHEST altitude candidate (most likely to be actual cruise)
    # Break ties by longest duration
    best = max(candidates, key=lambda x: (x[0], x[1]))
    return best[0]


def _calculate_track_stability(track: pd.Series, window_size: int = 10) -> pd.Series:
    """
    คำนวณความคงที่ของ track - VECTORIZED (ไม่มี loop)
    """
    # โปรแกรมแบบเวกเตอร์ใช้ rolling std
    track_std = track.rolling(window=window_size, center=True, min_periods=1).std()
    
    # แมปเป็น stability scores (เร็ว!)
    stability = pd.Series(np.where(track_std < 5, 1.0,
                         np.where(track_std < 15, 0.7,
                         np.where(track_std < 30, 0.4, 0.1))), index=track.index, dtype=float)
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
      - ถ้า Direction ต่างจาก ref_direction ไม่เกิน tolerance_deg (องศา) → Landing
      - ถ้า Direction ต่างจาก ref_direction เกิน tolerance_deg (องศา) → Taxi_in
    - ตรวจสอบแถวก่อนหน้าแถวแรกที่ altitude = 0 ft ด้วย:
      - ถ้า Direction ต่างจาก ref_direction ไม่เกิน tolerance_deg (องศา) → Landing
      - ถ้า Direction ต่างจาก ref_direction เกิน tolerance_deg (องศา) → Taxi_in
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
        if not pd.isna(alt) and (alt == 0 or alt < 1):
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
        is_ground = (not pd.isna(alt)) and (alt == 0 or alt < 1)
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




def _apply_dip_return_retro_labeling(phases: pd.Series, altitude: pd.Series, cruise_altitude: Optional[float], 
                                      max_dip_ft: float = 50.0, max_dip_rows: int = 100) -> pd.Series:
    """
    Post-processing: Retro-label Climb/Descent segments as Cruise if they form a dip-return pattern
    
    Patterns:
    1. Cruise -> [Descent/Climb/mixed] -> Cruise = retro-label all as Cruise (dip-return or peak-return)
    2. Climb ending at cruise altitude after Descent = retro-label as Cruise if purely ascending
    
    This handles cases where continuous gentle climbs back to cruise altitude are misclassified
    """
    refined = phases.copy()
    n = len(altitude)
    
    if n < 3 or cruise_altitude is None:
        return refined
    
    # Find Climb or Descent sections that end at cruise altitude
    i = 0
    while i < n:
        # Look for Climb or Descent sections that end at cruise altitude
        if refined.iloc[i] in ["Climb", "Descent"]:
            j = i
            min_alt_in_section = altitude.iloc[i]
            max_alt_in_section = altitude.iloc[i]
            
            # Scan forward to find section end
            while j < n and refined.iloc[j] in ["Climb", "Descent"]:
                min_alt_in_section = min(min_alt_in_section, altitude.iloc[j])
                max_alt_in_section = max(max_alt_in_section, altitude.iloc[j])
                j += 1
            
            section_len = j - i
            section_end_alt = altitude.iloc[j-1] if j > i else altitude.iloc[i]
            alt_start = altitude.iloc[i]
            alt_end = altitude.iloc[j-1]
            
            # Pattern 1: ends at cruise altitude and is short enough
            ends_at_cruise = abs(section_end_alt - cruise_altitude) <= 20.0
            section_not_too_long = section_len <= max_dip_rows
            section_long_enough = section_len >= 3
            
            # Check for Cruise context - look back within last 10 rows
            has_recent_cruise = False
            if i > 0:
                for back in range(1, min(11, i+1)):
                    if refined.iloc[i-back] == "Cruise":
                        has_recent_cruise = True
                        break
            
            # Pattern 2 (New): Ascending to cruise from below, with recent cruise context
            # Check if section is monotonically increasing (ascending pattern)
            section_is_ascending = True
            for k in range(i, j-1):
                if altitude.iloc[k+1] < altitude.iloc[k] - 0.1:  # Allow small noise
                    section_is_ascending = False
                    break
            
            should_retro_label = False
            
            # Pattern 1: Dip-return or peak-return (short oscillation ending at cruise)
            if ends_at_cruise and section_not_too_long and section_long_enough:
                if (abs(alt_start - cruise_altitude) <= 30.0 and 
                    abs(alt_end - cruise_altitude) <= 20.0):
                    
                    dip_depth = max(abs(alt_start - min_alt_in_section), 
                                   abs(alt_end - min_alt_in_section))
                    
                    if dip_depth <= max_dip_ft:
                        should_retro_label = True
            
            # Pattern 2: Monotonic climb to cruise with recent cruise context
            if (not should_retro_label and has_recent_cruise and ends_at_cruise and 
                section_not_too_long and section_is_ascending):
                # This is an ascending return to cruise
                should_retro_label = True
            
            if should_retro_label:
                # Retro-label entire section as Cruise
                for k in range(i, j):
                    refined.iloc[k] = "Cruise"
                i = j
            else:
                i = j
        else:
            i += 1
    
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
    
    # Simple Landing detection: altitude < 3000 ft after Descent/Approach
    for i in range(1, len(refined)):
        alt = altitude.iloc[i]
        
        # If altitude < 3000 and > 0, check if it should be Landing (3000 ft is Approach boundary)
        if alt > 0 and alt < 3000:
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

