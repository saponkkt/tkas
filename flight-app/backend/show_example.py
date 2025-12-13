"""
แสดงตัวอย่างผลการรัน detect_flight_phase()
"""
import pandas as pd
import numpy as np
from flight_phase import detect_flight_phase

# สร้างข้อมูลจำลอง
def create_sample_flight_data():
    """สร้างข้อมูลจำลองของเที่ยวบิน"""
    np.random.seed(42)  # ตั้งค่า seed เพื่อให้ผลลัพธ์เหมือนกันทุกครั้ง
    data = []
    
    # Taxi_out: altitude = 0, track เปลี่ยนไปมา (0-20 แถว)
    for i in range(20):
        data.append({
            "altitude": 0,
            "track": 45 + np.random.uniform(-30, 30),
            "ground_speed": 5 + np.random.uniform(-2, 2),
        })
    
    # Takeoff: altitude = 0, track คงที่ (21-25 แถว)
    takeoff_track = 90.0
    for i in range(5):
        data.append({
            "altitude": 0,
            "track": takeoff_track + np.random.uniform(-1, 1),
            "ground_speed": 50 + i * 10,
        })
    
    # Initial_climb: altitude 0-2000 ft (26-50 แถว)
    for i in range(25):
        alt = (i + 1) * 80
        data.append({
            "altitude": alt,
            "track": takeoff_track + np.random.uniform(-5, 5),
            "ground_speed": 120 + i * 5,
        })
    
    # Climb: altitude 2000 ถึง cruise (51-150 แถว)
    cruise_alt = 35000
    for i in range(100):
        alt = 2000 + (i + 1) * ((cruise_alt - 2000) / 100)
        data.append({
            "altitude": alt,
            "track": 90 + np.random.uniform(-10, 10),
            "ground_speed": 250 + i * 2,
        })
    
    # Cruise: altitude ซ้ำที่ 35000 ft (151-300 แถว)
    for i in range(150):
        data.append({
            "altitude": 35000,  # ซ้ำตัวเดิม
            "track": 90 + np.random.uniform(-5, 5),
            "ground_speed": 450 + np.random.uniform(-10, 10),
        })
    
    # Descent: altitude ลดลงจาก cruise ถึง 8000 ft (301-400 แถว)
    for i in range(100):
        alt = cruise_alt - (i + 1) * ((cruise_alt - 8000) / 100)
        data.append({
            "altitude": alt,
            "track": 90 + np.random.uniform(-10, 10),
            "ground_speed": 450 - i * 1.5,
        })
    
    # Approach: altitude 8000-3000 ft (401-450 แถว)
    for i in range(50):
        alt = 8000 - (i + 1) * ((8000 - 3000) / 50)
        data.append({
            "altitude": alt,
            "track": 90 + np.random.uniform(-15, 15),
            "ground_speed": 300 - i * 3,
        })
    
    # Landing: altitude 3000 ถึง 0 ft, track คงที่ (451-480 แถว)
    landing_track = 90.0
    for i in range(30):
        alt = 3000 - (i + 1) * (3000 / 30)
        data.append({
            "altitude": max(0, alt),
            "track": landing_track + np.random.uniform(-1, 1),
            "ground_speed": 150 - i * 4,
        })
    
    # Taxi_in: altitude = 0, track เปลี่ยนไปมา (481-500 แถว)
    for i in range(20):
        data.append({
            "altitude": 0,
            "track": 180 + np.random.uniform(-40, 40),
            "ground_speed": 10 + np.random.uniform(-3, 3),
        })
    
    return pd.DataFrame(data)


# สร้างข้อมูลและวิเคราะห์
print("=" * 100)
print("ตัวอย่างผลการรัน detect_flight_phase()")
print("=" * 100)
print()

df = create_sample_flight_data()
print(f"จำนวนข้อมูลทั้งหมด: {len(df)} แถว")
print()

# วิเคราะห์ flight phases
df_with_phases = detect_flight_phase(df, alt_col="altitude", track_col="track")

# แสดงสถิติ
print("=" * 100)
print("สถิติ Flight Phases")
print("=" * 100)
phase_counts = df_with_phases["flight_phase"].value_counts()
total = len(df_with_phases)

phase_order = ["Taxi_out", "Takeoff", "Initial_climb", "Climb", "Cruise", 
               "Descent", "Approach", "Landing", "Taxi_in"]

print(f"{'Phase':<20} {'จำนวนแถว':<15} {'เปอร์เซ็นต์':<15} {'ช่วง Altitude (ft)':<30}")
print("-" * 100)

for phase in phase_order:
    count = phase_counts.get(phase, 0)
    if count > 0:
        percentage = (count / total) * 100
        phase_data = df_with_phases[df_with_phases["flight_phase"] == phase]
        alt_min = phase_data["altitude"].min()
        alt_max = phase_data["altitude"].max()
        alt_range = f"{alt_min:.0f} - {alt_max:.0f}"
        print(f"{phase:<20} {count:<15} {percentage:>6.1f}%{'':<8} {alt_range:<30}")

print()

# แสดงตัวอย่างข้อมูลแต่ละ phase
print("=" * 100)
print("ตัวอย่างข้อมูลแต่ละ Flight Phase")
print("=" * 100)
print()

for phase in phase_order:
    phase_data = df_with_phases[df_with_phases["flight_phase"] == phase]
    if len(phase_data) > 0:
        print(f"\n{'='*100}")
        print(f"Phase: {phase}")
        print(f"จำนวนแถว: {len(phase_data)}")
        print(f"ช่วง Altitude: {phase_data['altitude'].min():.0f} - {phase_data['altitude'].max():.0f} ft")
        print(f"ค่าเฉลี่ย Altitude: {phase_data['altitude'].mean():.0f} ft")
        print(f"{'-'*100}")
        
        # แสดง 10 แถวแรก
        display_cols = ["altitude", "track", "ground_speed", "flight_phase"]
        sample = phase_data[display_cols].head(10)
        
        print("\nตัวอย่าง 10 แถวแรก:")
        print(sample.to_string(index=True))
        
        if len(phase_data) > 10:
            print(f"\n... และอีก {len(phase_data) - 10} แถว")
        
        # แสดงสถิติเพิ่มเติม
        if phase == "Cruise":
            # สำหรับ Cruise ตรวจสอบว่าค่าซ้ำกันจริงหรือไม่
            unique_alts = phase_data["altitude"].nunique()
            print(f"\nหมายเหตุ: Cruise phase มี altitude ที่แตกต่างกัน {unique_alts} ค่า")
            if unique_alts == 1:
                print("  [OK] ถูกต้อง: ค่า altitude ซ้ำกันทั้งหมด")
            else:
                print(f"  [INFO] มีการเปลี่ยนแปลงเล็กน้อย (tolerance ±50 ft)")
        
        print()

# แสดงลำดับ phases
print("=" * 100)
print("ลำดับ Flight Phases ในเที่ยวบิน")
print("=" * 100)
print()

# หา transition points
prev_phase = None
transitions = []
for i, row in df_with_phases.iterrows():
    current_phase = row["flight_phase"]
    if current_phase != prev_phase:
        transitions.append({
            "index": i,
            "phase": current_phase,
            "altitude": row["altitude"]
        })
        prev_phase = current_phase

print("ลำดับการเปลี่ยน Phase:")
for i, trans in enumerate(transitions[:15]):  # แสดง 15 แรก
    print(f"  {i+1:2d}. แถว {trans['index']:4d} -> {trans['phase']:20s} (altitude: {trans['altitude']:8.0f} ft)")

if len(transitions) > 15:
    print(f"  ... และอีก {len(transitions) - 15} การเปลี่ยน phase")

print()
print("=" * 100)
print("เสร็จสิ้น!")
print("=" * 100)

