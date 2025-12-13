import pandas as pd
import numpy as np
import xarray as xr
import datetime as dt
import math
import os
import time
import warnings
import glob

# ปิด SerializationWarning จาก xarray
warnings.filterwarnings("ignore", category=xr.SerializationWarning)

# ---------- CONFIG ----------
INPUT_FOLDER = r"C:\Users\User\Desktop\FUEL PROJECT\CSV\ThaiVietjet\A320\HS-VKA\3092025"   # <== เปลี่ยนโฟลเดอร์ที่เก็บไฟล์ CSV
OUTPUT_FOLDER = r"C:\Users\User\Desktop\FUEL PROJECT\TAS\ThaiVietjet\A320\HS-VKA\3092025\ERA5"  # <== เปลี่ยนโฟลเดอร์ที่ต้องการบันทึกผลลัพธ์
GFS_BASE = "https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"
USE_ERA5_IF_OLDER_THAN_DAYS = 9  # เกินนี้จะใช้ ERA5 (ต้องมี cdsapi)
GFS_RETRY_ATTEMPTS = 3  # จำนวนครั้งที่ลองเชื่อมต่อ GFS
GFS_RETRY_DELAY = 5  # เวลารอระหว่างลองเชื่อมต่อ (วินาที)
# สร้างโฟลเดอร์ผลลัพธ์หากยังไม่มี
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------- 1) GET ALL CSV FILES IN INPUT FOLDER ----------
csv_files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))
print(f"Found {len(csv_files)} CSV files in input folder")

# ---------- PROCESS EACH CSV FILE ----------
for input_csv in csv_files:
    print(f"\n{'='*50}")
    print(f"Processing: {os.path.basename(input_csv)}")
    print(f"{'='*50}")
    
    # สร้างชื่อไฟล์ผลลัพธ์
    base_name = os.path.splitext(os.path.basename(input_csv))[0]
    output_csv = os.path.join(OUTPUT_FOLDER, f"TAS_{base_name}.csv")
    
    # ---------- 2) LOAD & NORMALIZE ----------
    df = pd.read_csv(input_csv)
    
    # รีเนมคอลัมน์ให้ตรงกับที่ใช้
    rename_map = {
        "Timestamp": "time",
        "UTC": "utc_time",  # เก็บคอลัมน์ UTC ไว้ใช้แสดงผล
        "Altitude": "altitude",
        "Speed": "ground_speed",
        "Direction": "track",
        "Position": "Position"
    }
    df = df.rename(columns=rename_map)
    
    # แยก Position -> latitude, longitude (รองรับ comma หรือ space)
    if "Position" not in df.columns:
        print(f"⚠️ ไม่พบคอลัมน์ 'Position' ในไฟล์ {input_csv} - ข้ามไฟล์นี้")
        continue
        
    # ลองแยกด้วย comma ก่อน ถ้าไม่ได้ลอง space/semicolon
    split_cols = None
    for sep in [",", " ", ";", "|"]:
        try:
            split_cols = df["Position"].astype(str).str.split(sep, expand=True)
            if split_cols.shape[1] >= 2:
                break
        except Exception:
            pass

    if split_cols is None or split_cols.shape[1] < 2:
        print(f"⚠️ คอลัมน์ 'Position' ในไฟล์ {input_csv} ไม่มีรูปแบบที่ถูกต้อง - ข้ามไฟล์นี้")
        continue

    df["latitude"] = pd.to_numeric(split_cols[0], errors="coerce")
    df["longitude"] = pd.to_numeric(split_cols[1], errors="coerce")

    # ทำความสะอาด
    needed = ["time", "latitude", "longitude", "altitude", "ground_speed", "track"]
    for col in needed:
        if col not in df.columns:
            print(f"⚠️ ไม่พบคอลัมน์ที่ต้องใช้: {col} ในไฟล์ {input_csv} - ข้ามไฟล์นี้")
            continue

    # Timestamp: ถ้ามาเป็น ms ให้หาร 1000
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", "ground_speed", "track"])
    if len(df) == 0:
        print(f"⚠️ ไม่มีข้อมูลที่Validในไฟล์ {input_csv} - ข้ามไฟล์นี้")
        continue
        
    if df["time"].median() > 1e12:  # ms
        df["time"] = (df["time"] / 1000.0).astype(np.int64)
    else:  # s
        df["time"] = df["time"].astype(np.int64)

    # แปลง lon เป็นช่วง [0,360) สำหรับ GFS หากจำเป็น
    df.loc[df["longitude"] < 0, "longitude"] = df["longitude"] % 360.0

    # เวลาเที่ยวบินตัวอย่าง (ใช้แถวแรก) - แก้ไขการใช้ UTC
    flight_time = pd.to_datetime(df["time"].iloc[0], unit='s', utc=True)
    now_utc = pd.Timestamp.now(tz='UTC')

    # ---------- 3) PICK DATA SOURCE (GFS or ERA5) ----------
    use_era5 = (now_utc - flight_time).days > USE_ERA5_IF_OLDER_THAN_DAYS

    ds = None
    source = None

    if not use_era5:
        # NOAA GFS: เลือก run ใกล้ที่สุดแบบ floor (00/06/12/18Z)
        run_hour = (flight_time.hour // 6) * 6
        run_date = flight_time.strftime("%Y%m%d")
        run_str = f"{run_hour:02d}z"
        gfs_url = f"{GFS_BASE}/gfs{run_date}/gfs_0p25_1hr_{run_str}"
        print(f"📡 Using NOAA GFS run: {gfs_url}")
        
        # ลองเชื่อมต่อ GFS หลายครั้ง
        for attempt in range(1, GFS_RETRY_ATTEMPTS + 1):
            try:
                print(f"Attempt {attempt} to connect to GFS...")
                ds = xr.open_dataset(gfs_url)
                
                # แปลงเวลาใน dataset ให้เป็น timezone-aware UTC
                if 'time' in ds.coords and not hasattr(ds.time.dtype, 'tz'):
                    # ถ้าเวลาไม่มี timezone ให้เพิ่ม timezone เป็น UTC
                    ds['time'] = pd.to_datetime(ds['time'].values).tz_localize('UTC')
                
                source = "gfs"
                print("✅ Connected to GFS successfully")
                break
            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                if attempt < GFS_RETRY_ATTEMPTS:
                    print(f"Waiting {GFS_RETRY_DELAY} seconds before next attempt...")
                    time.sleep(GFS_RETRY_DELAY)
                else:
                    print("All GFS connection attempts failed → will try to use ERA5 instead")
                    use_era5 = True

    if use_era5:
        # ERA5 pressure levels (ต้องมี cdsapi และ ~/.cdsapirc)
        try:
            import cdsapi
            out_nc = f"era5_pressure_{flight_time.strftime('%Y%m%d')}.nc"
            if not os.path.exists(out_nc):
                print("📥 Downloading ERA5 pressure-level (u, v) สำหรับวันของ flight ...")
                c = cdsapi.Client()
                c.retrieve(
                    "reanalysis-era5-pressure-levels",
                    {
                        "product_type": "reanalysis",
                        "variable": ["u_component_of_wind", "v_component_of_wind"],
                        "pressure_level": ["100", "150", "200", "250", "300", "400", "500", "600", "700", "800", "850", "900", "925", "950", "975", "1000", "1013"],
                        "year": str(flight_time.year),
                        "month": f"{flight_time.month:02d}",
                        "day": f"{flight_time.day:02d}",
                        "time": [f"{h:02d}:00" for h in range(24)],
                        "format": "netcdf",
                    },
                    out_nc,
                )
            ds = xr.open_dataset(out_nc)
            
            # ตรวจสอบและใช้ dimension ที่ถูกต้องสำหรับ ERA5
            if 'valid_time' in ds.dims and 'time' not in ds.dims:
                # ใช้ valid_time แทน time
                ds = ds.rename({'valid_time': 'time'})
            
            # แปลงเวลาใน dataset ให้เป็น timezone-aware UTC
            if 'time' in ds.coords and not hasattr(ds.time.dtype, 'tz'):
                ds['time'] = pd.to_datetime(ds['time'].values).tz_localize('UTC')
            
            source = "era5"
            print("📡 Using ERA5 pressure levels local file:", out_nc)
            print("Dataset dimensions:", dict(ds.sizes))  # แก้ไขเป็น ds.sizes
        except Exception as e:
            print("เปิด ERA5 pressure levels ไม่ได้ → จะพยายามใช้ ERA5 single-level แทน:", e)
            # Fallback to single level
            try:
                out_nc = f"era5_single_level_{flight_time.strftime('%Y%m%d')}.nc"
                if not os.path.exists(out_nc):
                    print("📥 Downloading ERA5 single-level (u10, v10) สำหรับวันของ flight ...")
                    c = cdsapi.Client()
                    c.retrieve(
                        "reanalysis-era5-single-levels",
                        {
                            "product_type": "reanalysis",
                            "variable": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
                            "year": str(flight_time.year),
                            "month": f"{flight_time.month:02d}",
                            "day": f"{flight_time.day:02d}",
                            "time": [f"{h:02d}:00" for h in range(24)],
                            "format": "netcdf",
                        },
                        out_nc,
                    )
                ds = xr.open_dataset(out_nc)
                
                # ตรวจสอบและใช้ dimension ที่ถูกต้องสำหรับ ERA5
                if 'valid_time' in ds.dims and 'time' not in ds.dims:
                    # ใช้ valid_time แทน time
                    ds = ds.rename({'valid_time': 'time'})
                
                # แปลงเวลาใน dataset ให้เป็น timezone-aware UTC
                if 'time' in ds.coords and not hasattr(ds.time.dtype, 'tz'):
                    ds['time'] = pd.to_datetime(ds['time'].values).tz_localize('UTC')
                
                source = "era5_single"
                print("📡 Using ERA5 single level local file:", out_nc)
            except Exception as e2:
                print(f"⚠️ ไม่สามารถเปิด GFS และดาวน์โหลด ERA5 ไม่สำเร็จสำหรับไฟล์ {input_csv} — ข้ามไฟล์นี้")
                continue

    # ถ้าไม่สามารถโหลด dataset ได้ ให้ข้ามไฟล์นี้
    if ds is None:
        print(f"⚠️ ไม่สามารถโหลดข้อมูลลมสำหรับไฟล์ {input_csv} — ข้ามไฟล์นี้")
        continue

    # ---------- 4) WIND SAMPLING HELPERS ----------
    def pressure_hPa_from_alt_ft(alt_ft: float) -> float:
        """แปลงความสูง (ft) → ความดัน (hPa) ด้วย ISA คร่าวๆ"""
        alt_m = float(alt_ft) * 0.3048
        return 1013.25 * (1 - 0.0065 * alt_m / 288.15) ** 5.255

    def sample_wind(lat, lon, alt_ft, t_unix):
        """
        คืนค่า (u_kt, v_kt)
        - GFS: พยายามใช้ pressure-level ถ้ามี (ugrd/vgrd บน lev) หากไม่เจอค่อย fallback เป็น 10m
        - ERA5: ใช้ pressure levels ถ้ามี หรือ fallback เป็น single level
        """
        # แก้ไขการใช้ UTC - ใช้ pandas Timestamp แทน
        t_dt = pd.to_datetime(t_unix, unit='s', utc=True)
        
        # ตรวจสอบว่า dataset มี timezone หรือไม่ และแปลงหากจำเป็น
        if 'time' in ds.coords and not hasattr(ds.time.dtype, 'tz'):
            ds['time'] = pd.to_datetime(ds['time'].values).tz_localize('UTC')

        if source.startswith("gfs"):
            # เลือกเวลา/พิกัดที่ใกล้ที่สุด
            # GFS ใน NOMADS ใช้ coord: time, lat, lon ; pressure levels อาจเป็น 'lev'
            sel_time = ds.sel(time=t_dt, method="nearest")
            
            # ตรวจว่ามีตัวแปรระดับความดันไหม (ชื่ออาจเป็น 'ugrd' หรือ 'ugrdprs')
            # จะลองหลายชื่อเพื่อให้ robust
            var_candidates_u = [k for k in ds.variables if k.lower().startswith(("ugrd", "u-component_of_wind"))]
            var_candidates_v = [k for k in ds.variables if k.lower().startswith(("vgrd", "v-component_of_wind"))]

            # helper เลือกตัวแปร pressure-level ก่อน
            def pick_uv(vars_list):
                # ให้คะแนนตัวที่มีมิติ 'lev' หรือ 'isobaric' ก่อน
                pl = [v for v in vars_list if ("lev" in ds[v].dims) or any("isobar" in d for d in ds[v].dims)]
                if pl:
                    return pl[0]
                # มิฉะนั้นเลือก 10m (ugrd10m/vgrd10m)
                ten = [v for v in vars_list if "10m" in v.lower()]
                if ten:
                    return ten[0]
                return vars_list[0] if vars_list else None

            u_name = pick_uv(var_candidates_u)
            v_name = pick_uv(var_candidates_v)
            if (u_name is None) or (v_name is None):
                raise RuntimeError("ไม่พบตัวแปรลมใน GFS dataset")

            # เลือกระดับ: ถ้ามี 'lev' จะเลือกตามความดันใกล้เคียง
            da_u = sel_time[u_name]
            da_v = sel_time[v_name]

            # เลือกตำแหน่ง
            # บาง dataset ใช้พารามิเตอร์ชื่อ lat/lon หรือ latitude/longitude
            lat_name = "lat" if "lat" in ds.dims or "lat" in ds.coords else "latitude"
            lon_name = "lon" if "lon" in ds.dims or "lon" in ds.coords else "longitude"

            if "lev" in da_u.dims:
                p = pressure_hPa_from_alt_ft(alt_ft)
                point_u = da_u.sel({lat_name: lat, lon_name: lon, "lev": p}, method="nearest")
                point_v = da_v.sel({lat_name: lat, lon_name: lon, "lev": p}, method="nearest")
            else:
                point_u = da_u.sel({lat_name: lat, lon_name: lon}, method="nearest")
                point_v = da_v.sel({lat_name: lat, lon_name: lon}, method="nearest")

            u_ms = float(point_u.values)
            v_ms = float(point_v.values)
            u_kt = u_ms * 1.94384
            v_kt = v_ms * 1.94384
            return u_kt, v_kt

        elif source == "era5":
            # ERA5 pressure levels
            p = pressure_hPa_from_alt_ft(alt_ft)
            
            # ตรวจสอบชื่อ dimension ของระดับความดัน (อาจเป็น level, pressure, หรือ pressure_level)
            level_dims = [dim for dim in ds.dims if dim in ['level', 'pressure', 'pressure_level', 'isobaricInhPa']]
            if not level_dims:
                raise RuntimeError("ไม่พบ dimension ระดับความดันใน ERA5 dataset")
            
            level_name = level_dims[0]
            
            # ERA5 longitude ปกติ -180..180 — ถ้าเราเป็น 0..360 ให้แปลงกลับ
            lon_ = ((lon + 180) % 360) - 180
            
            time_sel = ds.sel(time=t_dt, method="nearest")
            
            # เลือกระดับความดันที่ใกล้เคียง
            point_u = time_sel['u'].sel({level_name: p, 'latitude': lat, 'longitude': lon_}, method="nearest")
            point_v = time_sel['v'].sel({level_name: p, 'latitude': lat, 'longitude': lon_}, method="nearest")
            
            u_ms = float(point_u.values)
            v_ms = float(point_v.values)
            return u_ms * 1.94384, v_ms * 1.94384

        else:
            # ERA5 single levels
            time_sel = ds.sel(time=t_dt, method="nearest")
            lat_name = "latitude" if "latitude" in ds.coords else "lat"
            lon_name = "longitude" if "longitude" in ds.coords else "lon"
            
            # ERA5 longitude ปกติ -180..180 — ถ้าเราเป็น 0..360 ให้แปลงกลับ
            lon_ = ((lon + 180) % 360) - 180
            
            # ลองหลายชื่อ
            cand_u = ["u10", "10m_u_component_of_wind"]
            cand_v = ["v10", "10m_v_component_of_wind"]
            u_name = next((c for c in cand_u if c in ds.variables), None)
            v_name = next((c for c in cand_v if c in ds.variables), None)
            
            if u_name is None or v_name is None:
                raise RuntimeError("ไม่พบตัวแปร u10/v10 ใน ERA5 ไฟล์")

            point_u = time_sel[u_name].sel({lat_name: lat, lon_name: lon_}, method="nearest")
            point_v = time_sel[v_name].sel({lat_name: lat, lon_name: lon_}, method="nearest")
            
            u_ms = float(point_u.values)
            v_ms = float(point_v.values)
            return u_ms * 1.94384, v_ms * 1.94384

    # ---------- 5) COMPUTE TAS ----------
    # หมายเหตุ: 'track' ในการบินวัดจาก "ทิศเหนือจริง" ตามเข็มนาฬิกา
    # ดังนั้นเวกเตอร์ GS:
    #   north = GS * cos(theta)
    #   east  = GS * sin(theta)
    # ส่วนลมจากโมเดล: u = eastward, v = northward
    tas_list = []
    u_list, v_list = [], []
    wind_speed_list = []
    wind_direction_list = []

    for i, row in df.iterrows():
        try:
            gs = float(row["ground_speed"])
            trk_deg = float(row["track"])
            if not np.isfinite(gs) or not np.isfinite(trk_deg):
                tas_list.append(np.nan)
                u_list.append(np.nan)
                v_list.append(np.nan)
                wind_speed_list.append(np.nan)
                wind_direction_list.append(np.nan)
                continue

            theta = math.radians(trk_deg)
            gs_north = gs * math.cos(theta)
            gs_east = gs * math.sin(theta)

            u_kt, v_kt = sample_wind(row["latitude"], row["longitude"], row["altitude"], row["time"])
            
            # คำนวณความเร็วและทิศทางลม
            wind_speed = math.hypot(u_kt, v_kt)
            wind_direction = (math.degrees(math.atan2(u_kt, v_kt)) + 360) % 360
            
            # TAS = GS - Wind
            tas_east = gs_east - u_kt
            tas_north = gs_north - v_kt
            tas = math.hypot(tas_east, tas_north)

            tas_list.append(tas)
            u_list.append(u_kt)
            v_list.append(v_kt)
            wind_speed_list.append(wind_speed)
            wind_direction_list.append(wind_direction)
            
        except Exception as e:
            tas_list.append(np.nan)
            u_list.append(np.nan)
            v_list.append(np.nan)
            wind_speed_list.append(np.nan)
            wind_direction_list.append(np.nan)
            print(f"แถว {i} เกิดปัญหา: {e}")

    df["wind_u_kt"] = u_list
    df["wind_v_kt"] = v_list
    df["wind_speed_kt"] = wind_speed_list
    df["wind_direction_deg"] = wind_direction_list
    df["TAS_kt"] = tas_list

    # ---------- 6) SAVE ----------
    # เพิ่มคอลัมน์ UTC ในผลลัพธ์
    df_out_cols = ["utc_time", "time", "latitude", "longitude", "altitude", 
                   "ground_speed", "track", "wind_u_kt", "wind_v_kt", 
                   "wind_speed_kt", "wind_direction_deg", "TAS_kt"]

    # บันทึกไฟล์ CSV
    df[df_out_cols].to_csv(output_csv, index=False)
    print(f"✅ Done. Saved: {output_csv}\nแหล่งลมที่ใช้: {source.upper()}")

    # แสดงตัวอย่างข้อมูล
    print("\n📊 ตัวอย่างข้อมูล 5 แถวแรก:")
    print(df[df_out_cols].head())
    
    # ปิด dataset เพื่อปล่อยทรัพยากร
    ds.close()

print("\n🎉 ประมวลผลทั้งหมดเสร็จสิ้น!")
