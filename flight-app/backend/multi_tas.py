import math
import os
import time  # ใช้สำหรับ progress bar
import warnings

import numpy as np
import pandas as pd
import streamlit as st
import xarray as xr

from calc import compute_tas_for_dataframe

warnings.filterwarnings("ignore")

st.set_page_config(page_title="TAS Calculator (Auto Source)", layout="wide")

# ==========================================
# 0. CONFIG & API CREDENTIALS
# ==========================================
THRESHOLD_DAYS = 9  # แก้เป็น 9 วัน (เพื่อให้ตรงกับ Logic ของไฟล์ Batch เดิม)

# --- ตั้งค่า API Key และ URL (ระบบใหม่ CDS-Beta) ---
ERA5_API_URL = "https://cds.climate.copernicus.eu/api"
ERA5_API_KEY = "21caa6d2-f9de-487f-af9a-1fa8337e7138"


# ต้องติดตั้ง cdsapi: pip install cdsapi
try:
    import cdsapi
except ImportError:
    st.error(
        "⚠️ ยังไม่ได้ติดตั้ง cdsapi กรุณารันคำสั่ง: pip install cdsapi ใน Terminal"
    )


# ==========================================
# 1. SERVICES ระดับ IO (ยังอยู่ในไฟล์นี้เพราะเกี่ยวข้องกับ Streamlit/UI)
# ==========================================


def get_wind_data_gfs(flight_time):
    """ดึงข้อมูลจาก NOAA GFS"""
    GFS_BASE = "https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"
    run_hour = (flight_time.hour // 6) * 6
    run_date = flight_time.strftime("%Y%m%d")
    run_str = f"{run_hour:02d}z"
    gfs_url = f"{GFS_BASE}/gfs{run_date}/gfs_0p25_1hr_{run_str}"

    st.info(f"📡 กำลังดึงข้อมูลจาก NOAA GFS (Server: {run_str})")
    try:
        ds = xr.open_dataset(gfs_url)
        if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
            ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
        return ds
    except Exception as e:
        st.error(f"เชื่อมต่อ GFS ไม่สำเร็จ: {e}")
        return None


def get_wind_data_era5(flight_time):
    """ดึงข้อมูลจาก ERA5"""
    date_str = flight_time.strftime("%Y%m%d")
    filename = f"era5_pressure_{date_str}.nc"

    st.info(f"📡 ตรวจสอบข้อมูล ERA5 สำหรับวันที่: {flight_time.strftime('%Y-%m-%d')}")

    # 1. ตรวจสอบไฟล์เก่า (Cache)
    if os.path.exists(filename):
        try:
            ds = xr.open_dataset(filename)
            _ = ds["u"].values[0]
            ds.close()
            st.success(f"📂 พบไฟล์เดิมในเครื่อง ({filename}) นำมาใช้ทันที")
            ds = xr.open_dataset(filename)
            if "valid_time" in ds.dims and "time" not in ds.dims:
                ds = ds.rename({"valid_time": "time"})
            if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
            return ds
        except Exception:
            try:
                os.remove(filename)
            except Exception:
                pass

    # 2. ดาวน์โหลดใหม่
    st.warning(
        "📥 กำลังดาวน์โหลดข้อมูลจาก ERA5 (ใช้เวลา 3-5 นาที)... ห้ามปิดหน้าจอ"
    )
    try:
        c = cdsapi.Client(url=ERA5_API_URL, key=ERA5_API_KEY)
        c.retrieve(
            "reanalysis-era5-pressure-levels",
            {
                "product_type": "reanalysis",
                "variable": ["u_component_of_wind", "v_component_of_wind"],
                "pressure_level": [
                    "100",
                    "150",
                    "200",
                    "250",
                    "300",
                    "400",
                    "500",
                    "600",
                    "700",
                    "800",
                    "850",
                    "900",
                    "925",
                    "950",
                    "1000",
                ],
                "year": str(flight_time.year),
                "month": f"{flight_time.month:02d}",
                "day": f"{flight_time.day:02d}",
                "time": [f"{h:02d}:00" for h in range(24)],
                "format": "netcdf",
            },
            filename,
        )
        st.success("✅ ดาวน์โหลด ERA5 เสร็จสิ้น!")
        ds = xr.open_dataset(filename)
        if "valid_time" in ds.dims and "time" not in ds.dims:
            ds = ds.rename({"valid_time": "time"})
        if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
            ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
        return ds
    except Exception as e:
        st.error(f"❌ โหลด ERA5 ไม่สำเร็จ: {e}")
        return None


# ==========================================
# 2. CONTROLLER LAYER
# ==========================================


def process_uploaded_file(uploaded_file):
    """
    Controller:
    - อ่านไฟล์ CSV
    - clean/เตรียมข้อมูล
    - เลือกแหล่งข้อมูลลม (GFS/ERA5)
    - เรียก service compute_tas_for_dataframe จาก calc.py
    - จัดการ progress bar และแสดงผล
    """
    uploaded_file.seek(0)
    df = pd.read_csv(uploaded_file)

    # Clean Data (เหมือนเดิม)
    rename_map = {
        "Timestamp": "time",
        "UTC": "utc_time",
        "Altitude": "altitude",
        "Speed": "ground_speed",
        "Direction": "track",
        "Position": "Position",
    }
    df = df.rename(columns=rename_map)
    if "Position" in df.columns:
        split = df["Position"].astype(str).str.split(",", expand=True)
        if split.shape[1] >= 2:
            df["latitude"] = pd.to_numeric(split[0], errors="coerce")
            df["longitude"] = pd.to_numeric(split[1], errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", "ground_speed"])
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    if df["time"].median() > 1e12:
        df["time"] = (df["time"] / 1000.0).astype(np.int64)
    df.loc[df["longitude"] < 0, "longitude"] = df["longitude"] % 360.0

    if len(df) == 0:
        st.error("ไม่พบข้อมูลที่ใช้งานได้ในไฟล์")
        st.stop()

    # Check Date
    flight_time = pd.to_datetime(df["time"].iloc[0], unit="s", utc=True)
    now_utc = pd.Timestamp.now(tz="UTC")
    days_diff = (now_utc - flight_time).days

    source_code = "GFS"
    ds = None

    st.markdown("---")
    st.write(
        f"📅 วันที่บิน: **{flight_time.strftime('%Y-%m-%d')}** (ผ่านมาแล้ว {days_diff} วัน)"
    )

    if days_diff > THRESHOLD_DAYS:
        source_code = "ERA5"
        st.warning(
            f"⚡ ไฟล์เก่าเกิน {THRESHOLD_DAYS} วัน -> ระบบสลับไปใช้ **ERA5** (แม่นยำสูง)"
        )
        ds = get_wind_data_era5(flight_time)
    else:
        source_code = "GFS"
        st.success(
            f"⚡ ไฟล์ใหม่ -> ระบบเลือกใช้ **NOAA GFS** (รวดเร็ว)"
        )
        ds = get_wind_data_gfs(flight_time)

    # ======================================
    # จุดสำคัญ: Progress Bar (ยังอยู่ฝั่ง controller/UI)
    # ======================================
    if ds is None:
        return

    st.markdown("### ⏳ Status")
    progress_text = st.empty()
    progress_bar = st.progress(0)

    total_rows = len(df)
    start_time = time.time()

    # ใช้ loop เพื่ออัปเดต progress แต่ delegate การคำนวณจริงให้ service
    # ตรงนี้เราจะค่อยๆ เพิ่มสัดส่วน แต่คำนวณ TAS ทั้งก้อนทีเดียวเพื่อให้โค้ดง่าย
    for i in range(total_rows):
        if i % max(1, total_rows // 20) == 0 or i == total_rows - 1:
            percent = int(((i + 1) / total_rows) * 100)
            progress_bar.progress(min((i + 1) / total_rows, 1.0))
            progress_text.markdown(
                f"**กำลังเตรียมข้อมูล... {percent}%** ({i+1}/{total_rows} แถว)"
            )

    # เรียก service compute_tas_for_dataframe จาก calc.py
    df_out = compute_tas_for_dataframe(df, ds, source_code)

    progress_bar.progress(1.0)
    progress_text.success("✅ ประมวลผลเสร็จสิ้น! (100%)")

    st.write(f"แหล่งข้อมูลลมที่ใช้: **{source_code}**")
    st.dataframe(
        df_out[
            [
                "time",
                "latitude",
                "longitude",
                "altitude",
                "TAS_kt",
                "Wind_Speed_kt",
            ]
        ].head()
    )

    csv = df_out.to_csv(index=False).encode("utf-8")
    out_name = f"TAS_{uploaded_file.name}"
    st.download_button("📥 ดาวน์โหลด CSV", csv, out_name, "text/csv")

    ds.close()


# ==========================================
# 3. ROUTER LAYER (entry point ของ Streamlit app)
# ==========================================


def router():
    """Router หลักของหน้า Streamlit (กำหนด layout + mapping ไป controller)"""
    st.title("✈️ Flight TAS Calculator (Auto-Switch)")
    st.info(
        f"ℹ️ **ระบบอัตโนมัติ:** ถ้าไฟล์เก่าเกิน **{THRESHOLD_DAYS} วัน** "
        f"จะใช้ **ERA5** (แม่นยำ), ถ้าเป็นไฟล์ใหม่จะใช้ **GFS** (เร็ว)"
    )

    uploaded_file = st.file_uploader(
        "เลือกไฟล์ CSV (FlightRadar24)", type=["csv"]
    )

    if uploaded_file and st.button("🚀 เริ่มประมวลผล"):
        process_uploaded_file(uploaded_file)


# ให้ Streamlit เรียกใช้ router() เป็น entry point
if __name__ == "__main__":
    router()
