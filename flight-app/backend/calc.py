from __future__ import annotations

from io import IOBase
from typing import BinaryIO, Dict, Union

import math

import numpy as np
import pandas as pd

from haversine import haversine_nm


# ============================================================================
# Services สำหรับ endpoint /upload (ระยะทาง, fuel, mass, CO2)
# ============================================================================


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    ตรวจสอบให้แน่ใจว่า CSV มีคอลัมน์ที่ต้องใช้: lat, lon, altitude, timestamp
    และจัดการ dtype ให้เหมาะสม
    """
    required = ["lat", "lon", "altitude", "timestamp"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = df.copy()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["altitude"] = pd.to_numeric(df["altitude"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["lat", "lon", "timestamp"])
    return df


def compute_flight_metrics_from_csv(
    file_obj: Union[BinaryIO, IOBase]
) -> Dict[str, float]:
    """
    อ่าน FlightRadar24-like CSV และคำนวณ distance, fuel, mass, CO2
    ใช้สำหรับ backend FastAPI /upload
    """
    df = pd.read_csv(file_obj)
    df = _ensure_required_columns(df)

    if len(df) < 2:
        # Not enough points to form a segment
        return {
            "distance_nm": 0.0,
            "fuel_kg": 0.0,
            "mass_kg": 0.0,
            "co2_kg": 0.0,
        }

    # Sort by timestamp to get real path order if not already sorted
    df = df.sort_values("timestamp").reset_index(drop=True)

    total_distance_nm = 0.0
    for i in range(1, len(df)):
        lat1, lon1 = float(df.loc[i - 1, "lat"]), float(df.loc[i - 1, "lon"])
        lat2, lon2 = float(df.loc[i, "lat"]), float(df.loc[i, "lon"])
        total_distance_nm += haversine_nm(lat1, lon1, lat2, lon2)

    fuel_kg = total_distance_nm * 4.2
    mass_kg = fuel_kg * 1.05
    co2_kg = fuel_kg * 3.16

    return {
        "distance_nm": round(total_distance_nm, 3),
        "fuel_kg": round(fuel_kg, 3),
        "mass_kg": round(mass_kg, 3),
        "co2_kg": round(co2_kg, 3),
    }


# ============================================================================
# Services สำหรับ TAS calculator (ย้าย logic ออกจาก multi_tas.py)
# ============================================================================


def pressure_hPa_from_alt_ft(alt_ft: float) -> float:
    """แปลงความสูง (ft) -> ความดัน (hPa)"""
    alt_m = float(alt_ft) * 0.3048
    return 1013.25 * (1 - 0.0065 * alt_m / 288.15) ** 5.255


def sample_wind(ds, source_type: str, lat: float, lon: float, alt_ft: float, t_unix: float):
    """
    ฟังก์ชัน core สำหรับสุ่มค่าลมจาก dataset (GFS / ERA5)
    แยกออกมาเป็น service ไม่ผูกกับ Streamlit
    """
    t_dt = pd.to_datetime(t_unix, unit="s", utc=True)
    sel_time = ds.sel(time=t_dt, method="nearest")

    u_kt, v_kt = np.nan, np.nan
    try:
        p = pressure_hPa_from_alt_ft(alt_ft)
        if source_type == "GFS":
            var_u = [k for k in ds.variables if k.startswith("ugrd") and "lev" in ds[k].dims][
                0
            ]
            var_v = [k for k in ds.variables if k.startswith("vgrd") and "lev" in ds[k].dims][
                0
            ]
            lat_n = "lat" if "lat" in ds.coords else "latitude"
            lon_n = "lon" if "lon" in ds.coords else "longitude"
            point_u = sel_time[var_u].sel(
                {lat_n: lat, lon_n: lon, "lev": p}, method="nearest"
            )
            point_v = sel_time[var_v].sel(
                {lat_n: lat, lon_n: lon, "lev": p}, method="nearest"
            )
        elif source_type == "ERA5":
            lon_era = ((lon + 180) % 360) - 180
            lev_n = next(
                (d for d in ds.dims if d in ["level", "pressure_level", "isobaricInhPa"]),
                None,
            )
            point_u = sel_time["u"].sel(
                {lev_n: p, "latitude": lat, "longitude": lon_era}, method="nearest"
            )
            point_v = sel_time["v"].sel(
                {lev_n: p, "latitude": lat, "longitude": lon_era}, method="nearest"
            )
        else:
            return np.nan, np.nan

        u_kt = float(point_u.values) * 1.94384
        v_kt = float(point_v.values) * 1.94384
    except Exception:
        pass
    return u_kt, v_kt


def compute_tas_for_dataframe(df: pd.DataFrame, ds, source_type: str) -> pd.DataFrame:
    """
    Service function:
    รับ DataFrame ที่เตรียมแล้ว + dataset ลม (ds) + source_type (GFS / ERA5)
    คืน DataFrame เดิมที่เพิ่มคอลัมน์ TAS_kt และ Wind_Speed_kt
    """
    tas_list, ws_list = [], []

    for _, row in df.iterrows():
        try:
            gs = float(row["ground_speed"])
            trk = float(row["track"])
            theta = math.radians(trk)

            u, v = sample_wind(
                ds,
                source_type,
                row["latitude"],
                row["longitude"],
                row["altitude"],
                row["time"],
            )

            if np.isnan(u):
                tas_list.append(np.nan)
                ws_list.append(np.nan)
            else:
                gs_n = gs * math.cos(theta)
                gs_e = gs * math.sin(theta)
                tas = math.hypot(gs_e - u, gs_n - v)
                tas_list.append(tas)
                ws_list.append(math.hypot(u, v))
        except Exception:
            tas_list.append(np.nan)
            ws_list.append(np.nan)

    df_out = df.copy()
    df_out["TAS_kt"] = tas_list
    df_out["Wind_Speed_kt"] = ws_list
    return df_out


# ============================================================================
# Temperature helpers (ERA5 + barometric lapse rate)
# ============================================================================


def _find_temperature_variable(ds):
    """Return the first plausible temperature variable name in an ERA5 dataset."""
    preferred = ["t2m", "t", "temperature"]
    for name in preferred:
        if name in ds.variables:
            return name
    return next((v for v in ds.variables if "temp" in v.lower()), None)


def sample_temperature_era5(ds, lat: float, lon: float, t_unix: float) -> float:
    """
    ดึงอุณหภูมิ (K) ที่ระดับน้ำทะเลจาก ERA5 ตามตำแหน่ง/เวลา
    ใช้แค่แถวแรกของไฟล์ ADS-B (sea-level assumption)
    """
    t_dt = pd.to_datetime(t_unix, unit="s", utc=True)
    sel_time = ds.sel(time=t_dt, method="nearest")

    lon_era = ((lon + 180) % 360) - 180
    temp_var = _find_temperature_variable(ds)
    if temp_var is None:
        return np.nan

    try:
        # ถ้ามีมิติระดับความกด เลือก 0 ft -> ~1013 hPa
        if any(dim in ds[temp_var].dims for dim in ["level", "pressure_level", "isobaricInhPa"]):
            lev_n = next(
                (d for d in ds[temp_var].dims if d in ["level", "pressure_level", "isobaricInhPa"]),
                None,
            )
            p0 = pressure_hPa_from_alt_ft(0.0)
            point_t = sel_time[temp_var].sel(
                {lev_n: p0, "latitude": lat, "longitude": lon_era}, method="nearest"
            )
        else:
            lat_n = "latitude" if "latitude" in ds[temp_var].dims else "lat"
            lon_n = "longitude" if "longitude" in ds[temp_var].dims else "lon"
            point_t = sel_time[temp_var].sel({lat_n: lat, lon_n: lon_era}, method="nearest")

        return float(point_t.values)
    except Exception:
        return np.nan


def compute_temperature_profile_from_adsb_csv(
    file_obj: Union[BinaryIO, IOBase], era5_ds
) -> pd.DataFrame:
    """
    อ่านไฟล์ ADS-B (.csv) แล้วคำนวณอุณหภูมิแต่ละแถว

    - ใช้ ERA5 หา T0 ที่ระดับน้ำทะเลจาก "บรรทัดแรก" ของไฟล์
    - แถวถัดไปใช้อสมการ lapse rate: T = T0 - (L * h)
      L = 0.0065 K/m, h จาก altitude (ft -> m)
    """
    df = pd.read_csv(file_obj)
    df = _ensure_required_columns(df)

    if df.empty:
        return df.assign(temperature_K=np.nan)

    first_ts = df.loc[0, "timestamp"]
    t_unix = pd.Timestamp(first_ts).timestamp()
    t0 = sample_temperature_era5(
        era5_ds, float(df.loc[0, "lat"]), float(df.loc[0, "lon"]), t_unix
    )

    if np.isnan(t0):
        # ถ้าอ่าน ERA5 ไม่ได้ ให้คืนคอลัมน์ว่างเพื่อไม่หยุดการทำงาน
        return df.assign(temperature_K=np.nan)

    L = 0.0065  # K/m
    alt_m = df["altitude"].astype(float) * 0.3048
    df_out = df.copy()
    df_out["temperature_K"] = t0 - (L * alt_m)
    return df_out