import os
import math
import time
import warnings
from typing import Tuple

import pandas as pd
import numpy as np
import xarray as xr
import traceback

# ปิด SerializationWarning จาก xarray
warnings.filterwarnings("ignore", category=xr.SerializationWarning)

# Embedded CDS API credentials (user requested to hard-code for this project)
# Use the correct base API URL (no /v2) so cdsapi constructs valid endpoints.
CDSAPI_URL = "https://cds.climate.copernicus.eu/api"
CDSAPI_KEY = "21caa6d2-f9de-487f-af9a-1fa8337e7138"


def _sanitize_cds_key(k: str) -> str:
    """Normalize a user-supplied or embedded CDS API key.

    - Strip surrounding angle brackets
    - If a deprecated UID prefix is present (e.g. '<UID>:KEY' or 'UID:KEY'), drop the UID
    """
    if k is None:
        return k
    s = str(k).strip()
    s = s.strip('<> ').strip()
    if ':' in s:
        left, right = s.split(':', 1)
        if len(left) < 64:
            s = right.strip()
    return s


GFS_BASE = "https://nomads.ncep.noaa.gov/dods/gfs_0p25_1hr"
USE_ERA5_IF_OLDER_THAN_DAYS = 9
GFS_RETRY_ATTEMPTS = 3
GFS_RETRY_DELAY = 5


def _is_valid_netcdf(path: str) -> bool:
    """Quickly validate a NetCDF/HDF5 file by attempting to open it with
    either netCDF4 or h5py. Returns True if readable, False otherwise.
    """
    try:
        from netCDF4 import Dataset as _Dataset

        d = _Dataset(path, "r")
        d.close()
        return True
    except Exception:
        try:
            import h5py

            f = h5py.File(path, "r")
            f.close()
            return True
        except Exception:
            return False


def pressure_hPa_from_alt_ft(alt_ft: float) -> float:
    alt_m = float(alt_ft) * 0.3048
    return 1013.25 * (1 - 0.0065 * alt_m / 288.15) ** 5.255


def _normalize_input_df(df: pd.DataFrame) -> pd.DataFrame:
    # case-insensitive normalization: map common variants to canonical names
    col_map = {}
    lower_to_actual = {c.lower(): c for c in df.columns}

    def pick(*choices):
        for ch in choices:
            if ch.lower() in lower_to_actual:
                return lower_to_actual[ch.lower()]
        return None

    # time / timestamp
    tcol = pick('time', 'timestamp', 'ts')
    if tcol:
        col_map[tcol] = 'time'

    # utc
    ucol = pick('utc', 'utc_time', 'UTC')
    if ucol:
        col_map[ucol] = 'utc_time'

    # altitude variants
    acol = pick('altitude', 'alt', 'elevation', 'height', 'alt_ft', 'alt_m')
    if acol:
        col_map[acol] = 'altitude'

    # speed
    scol = pick('ground_speed', 'speed', 'gs')
    if scol:
        col_map[scol] = 'ground_speed'

    # track / heading / direction
    trcol = pick('track', 'direction', 'heading')
    if trcol:
        col_map[trcol] = 'track'

    # lat/lon
    latc = pick('latitude', 'lat')
    lonc = pick('longitude', 'lon', 'long')
    if latc:
        col_map[latc] = 'latitude'
    if lonc:
        col_map[lonc] = 'longitude'

    # position-like
    posc = pick('position', 'pos', 'latlon', 'lat_long')
    if posc:
        col_map[posc] = 'Position'

    # apply renames
    if col_map:
        df = df.rename(columns=col_map)

    # parse Position into lat/lon if present
    if 'Position' in df.columns and ('latitude' not in df.columns or 'longitude' not in df.columns):
        split_cols = None
        for sep in [',', ' ', ';', '|']:
            try:
                split_cols = df['Position'].astype(str).str.split(sep, expand=True)
                if split_cols.shape[1] >= 2:
                    break
            except Exception:
                pass
        if split_cols is not None and split_cols.shape[1] >= 2:
            df['latitude'] = pd.to_numeric(split_cols[0], errors='coerce')
            df['longitude'] = pd.to_numeric(split_cols[1], errors='coerce')

    # ensure required cols exist (create with NaN defaults)
    required = ['time', 'latitude', 'longitude', 'altitude', 'ground_speed', 'track']
    for c in required:
        if c not in df.columns:
            df[c] = np.nan

    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", "ground_speed", "track"]) 
    if len(df) == 0:
        return df

    # ms -> s
    if df["time"].median() > 1e12:
        df["time"] = (df["time"] / 1000.0).astype(np.int64)
    else:
        df["time"] = df["time"].astype(np.int64)

    # convert longitude for GFS (0..360)
    df.loc[df["longitude"] < 0, "longitude"] = df["longitude"] % 360.0

    return df


def compute_tas_for_dataframe(df: pd.DataFrame, prefer_gfs: bool = True, input_csv: str | None = None) -> pd.DataFrame:
    """Compute wind/TAS for each row of `df` and return DataFrame with exact columns.

    This function does not read/write files. It may attempt to open GFS (remote)
    and/or download/read ERA5 netCDF if needed (requires `cdsapi`).
    """
    df = df.copy()
    df = _normalize_input_df(df)
    if df.empty:
        return df

    flight_time = pd.to_datetime(df["time"].iloc[0], unit="s", utc=True)
    now_utc = pd.Timestamp.now(tz="UTC")
    use_era5 = (now_utc - flight_time).days > USE_ERA5_IF_OLDER_THAN_DAYS

    ds = None
    source = None

    if prefer_gfs and not use_era5:
        run_hour = (flight_time.hour // 6) * 6
        run_date = flight_time.strftime("%Y%m%d")
        run_str = f"{run_hour:02d}z"
        gfs_url = f"{GFS_BASE}/gfs{run_date}/gfs_0p25_1hr_{run_str}"
        for attempt in range(1, GFS_RETRY_ATTEMPTS + 1):
            try:
                ds = xr.open_dataset(gfs_url)
                if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                    ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
                source = "gfs"
                break
            except Exception:
                time.sleep(GFS_RETRY_DELAY)
        if ds is None:
            use_era5 = True

    if use_era5 and ds is None:
        try:
            import cdsapi  # type: ignore
            out_nc = f"era5_pressure_{flight_time.strftime('%Y%m%d')}.nc"
            if not os.path.exists(out_nc):
                # Prefer using cdsapi.Client() which reads ~/.cdsapirc or env vars.
                cds_url = os.getenv("CDSAPI_URL", CDSAPI_URL)
                cds_key = os.getenv("CDSAPI_KEY", CDSAPI_KEY)
                if not cds_key:
                    _uid = os.getenv("CDSAPI_UID")
                    _secret = os.getenv("CDSAPI_API_KEY")
                    if _uid and _secret:
                        cds_key = f"{_uid}:{_secret}"

                if cds_key:
                    cds_key = _sanitize_cds_key(cds_key)
                    # Export to env to encourage cdsapi to pick it up
                    os.environ["CDSAPI_KEY"] = cds_key
                    os.environ["CDSAPI_URL"] = cds_url

                try:
                    # Try default client (uses ~/.cdsapirc or env vars)
                    c = cdsapi.Client()
                except Exception:
                    # Last resort: try explicit constructor
                    try:
                        c = cdsapi.Client(url=cds_url, key=cds_key)
                    except Exception as e:
                        print("Unable to construct cdsapi.Client():", e)
                        print("If you have an interfering package (ecmwf-datastores), consider uninstalling it.")
                        raise

                try:
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
                except Exception as e:
                    print("ERA5 pressure-level retrieve failed:", e)
                    print(traceback.format_exc())
            # attempt to open the downloaded file (if exists)
            try:
                ds = xr.open_dataset(out_nc)
                if "valid_time" in ds.dims and "time" not in ds.dims:
                    ds = ds.rename({"valid_time": "time"})
                if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                    ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
                source = "era5"
            except Exception as e:
                print(f"Failed to open ERA5 pressure-level file {out_nc}: {e}")
                try:
                    st = os.stat(out_nc)
                    print(f"File exists: {out_nc}, size={st.st_size} bytes")
                except Exception:
                    print(f"File not present: {out_nc}")
                print(traceback.format_exc())

                # If the file exists but is malformed/corrupted, remove it and
                # retry the CDS retrieve once. This handles partial/incomplete
                # downloads which otherwise raise HDF errors when xarray/netCDF4
                # try to open them.
                try:
                    if os.path.exists(out_nc) and not _is_valid_netcdf(out_nc):
                        print(f"Detected corrupted NetCDF {out_nc}, removing and retrying download")
                        try:
                            os.remove(out_nc)
                        except Exception:
                            pass
                        try:
                            # retry retrieve
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
                            if "valid_time" in ds.dims and "time" not in ds.dims:
                                ds = ds.rename({"valid_time": "time"})
                            if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                                ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
                            source = "era5"
                        except Exception as e2:
                            print("Retry after removing corrupted file failed:", e2)
                            print(traceback.format_exc())
                            ds = None
                    else:
                        ds = None
                except Exception:
                    ds = None
        except Exception:
            # fallback to single-level
            try:
                import cdsapi  # type: ignore
                out_nc = f"era5_single_level_{flight_time.strftime('%Y%m%d')}.nc"
                if not os.path.exists(out_nc):
                    cds_url = os.getenv("CDSAPI_URL", CDSAPI_URL)
                    cds_key = os.getenv("CDSAPI_KEY", CDSAPI_KEY)
                    if not cds_key:
                        _uid = os.getenv("CDSAPI_UID")
                        _secret = os.getenv("CDSAPI_API_KEY")
                        if _uid and _secret:
                            cds_key = f"{_uid}:{_secret}"

                    if cds_key:
                        cds_key = _sanitize_cds_key(cds_key)
                        os.environ["CDSAPI_KEY"] = cds_key
                        os.environ["CDSAPI_URL"] = cds_url

                    try:
                        c = cdsapi.Client()
                    except Exception:
                        try:
                            c = cdsapi.Client(url=cds_url, key=cds_key)
                        except Exception as e:
                            print("Unable to construct cdsapi.Client() for single-level ERA5:", e)
                            print("If you have an interfering package (ecmwf-datastores), consider uninstalling it.")
                            raise

                    try:
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
                    except Exception as e:
                        print("ERA5 single-level retrieve failed:", e)
                        print(traceback.format_exc())
                try:
                    ds = xr.open_dataset(out_nc)
                    if "valid_time" in ds.dims and "time" not in ds.dims:
                        ds = ds.rename({"valid_time": "time"})
                    if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                        ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
                    source = "era5_single"
                except Exception as e:
                    print(f"Failed to open ERA5 single-level file {out_nc}: {e}")
                    try:
                        st = os.stat(out_nc)
                        print(f"File exists: {out_nc}, size={st.st_size} bytes")
                    except Exception:
                        print(f"File not present: {out_nc}")
                    print(traceback.format_exc())

                    # Validate and possibly remove corrupted file, then retry once
                    try:
                        if os.path.exists(out_nc) and not _is_valid_netcdf(out_nc):
                            print(f"Detected corrupted NetCDF {out_nc}, removing and retrying download (single-level)")
                            try:
                                os.remove(out_nc)
                            except Exception:
                                pass
                            try:
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
                                if "valid_time" in ds.dims and "time" not in ds.dims:
                                    ds = ds.rename({"valid_time": "time"})
                                if "time" in ds.coords and not hasattr(ds.time.dtype, "tz"):
                                    ds["time"] = pd.to_datetime(ds["time"].values).tz_localize("UTC")
                                source = "era5_single"
                            except Exception as e2:
                                print("Retry after removing corrupted single-level file failed:", e2)
                                print(traceback.format_exc())
                                ds = None
                        else:
                            ds = None
                    except Exception:
                        ds = None
            except Exception as e:
                print("ERA5 single-level block failed:", e)
                print(traceback.format_exc())
                ds = None

    if ds is None:
        # cannot sample winds; fail fast because downstream calculations
        # (TAS -> thrust/fuel) depend on wind data being present.
        raise RuntimeError(
            "Unable to load wind data: GFS unreachable and ERA5 download/open failed. "
            "Ensure network access to GFS or configure cdsapi and a valid ~/.cdsapirc for ERA5 downloads."
        )

    # define sample_wind closure using ds & source
    def sample_wind(lat, lon, alt_ft, t_unix) -> Tuple[float, float]:
        t_dt = pd.to_datetime(t_unix, unit='s', utc=True)
        if 'time' in ds.coords and not hasattr(ds.time.dtype, 'tz'):
            ds['time'] = pd.to_datetime(ds['time'].values).tz_localize('UTC')

        if source.startswith('gfs'):
            sel_time = ds.sel(time=t_dt, method='nearest')
            var_candidates_u = [k for k in ds.variables if k.lower().startswith(('ugrd', 'u-component_of_wind'))]
            var_candidates_v = [k for k in ds.variables if k.lower().startswith(('vgrd', 'v-component_of_wind'))]

            def pick_uv(vars_list):
                pl = [v for v in vars_list if ('lev' in ds[v].dims) or any('isobar' in d for d in ds[v].dims)]
                if pl:
                    return pl[0]
                ten = [v for v in vars_list if '10m' in v.lower()]
                if ten:
                    return ten[0]
                return vars_list[0] if vars_list else None

            u_name = pick_uv(var_candidates_u)
            v_name = pick_uv(var_candidates_v)
            if (u_name is None) or (v_name is None):
                raise RuntimeError('no wind vars in GFS')

            da_u = sel_time[u_name]
            da_v = sel_time[v_name]
            lat_name = 'lat' if 'lat' in ds.dims or 'lat' in ds.coords else 'latitude'
            lon_name = 'lon' if 'lon' in ds.dims or 'lon' in ds.coords else 'longitude'

            if 'lev' in da_u.dims:
                p = pressure_hPa_from_alt_ft(alt_ft)
                point_u = da_u.sel({lat_name: lat, lon_name: lon, 'lev': p}, method='nearest')
                point_v = da_v.sel({lat_name: lat, lon_name: lon, 'lev': p}, method='nearest')
            else:
                point_u = da_u.sel({lat_name: lat, lon_name: lon}, method='nearest')
                point_v = da_v.sel({lat_name: lat, lon_name: lon}, method='nearest')

            u_ms = float(point_u.values)
            v_ms = float(point_v.values)
            return u_ms * 1.94384, v_ms * 1.94384

        if source == 'era5':
            p = pressure_hPa_from_alt_ft(alt_ft)
            level_dims = [dim for dim in ds.dims if dim in ['level', 'pressure', 'pressure_level', 'isobaricInhPa']]
            if not level_dims:
                raise RuntimeError('no pressure level dim in ERA5')
            level_name = level_dims[0]
            lon_ = ((lon + 180) % 360) - 180
            time_sel = ds.sel(time=t_dt, method='nearest')
            point_u = time_sel['u'].sel({level_name: p, 'latitude': lat, 'longitude': lon_}, method='nearest')
            point_v = time_sel['v'].sel({level_name: p, 'latitude': lat, 'longitude': lon_}, method='nearest')
            u_ms = float(point_u.values)
            v_ms = float(point_v.values)
            return u_ms * 1.94384, v_ms * 1.94384

        # era5 single
        time_sel = ds.sel(time=t_dt, method='nearest')
        lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
        lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
        lon_ = ((lon + 180) % 360) - 180
        cand_u = ['u10', '10m_u_component_of_wind']
        cand_v = ['v10', '10m_v_component_of_wind']
        u_name = next((c for c in cand_u if c in ds.variables), None)
        v_name = next((c for c in cand_v if c in ds.variables), None)
        if u_name is None or v_name is None:
            raise RuntimeError('no u10/v10 in ERA5 single-level')
        point_u = time_sel[u_name].sel({lat_name: lat, lon_name: lon_}, method='nearest')
        point_v = time_sel[v_name].sel({lat_name: lat, lon_name: lon_}, method='nearest')
        u_ms = float(point_u.values)
        v_ms = float(point_v.values)
        return u_ms * 1.94384, v_ms * 1.94384

    # compute per-row
    tas_list = []
    u_list = []
    v_list = []
    wind_speed_list = []
    wind_direction_list = []

    for i, row in df.iterrows():
        try:
            gs = float(row['ground_speed'])
            trk_deg = float(row['track'])
            if not np.isfinite(gs) or not np.isfinite(trk_deg):
                raise ValueError('invalid gs/track')

            theta = math.radians(trk_deg)
            gs_north = gs * math.cos(theta)
            gs_east = gs * math.sin(theta)

            u_kt, v_kt = sample_wind(row['latitude'], row['longitude'], row.get('altitude', 0.0), row['time'])
            wind_speed = math.hypot(u_kt, v_kt)
            wind_direction = (math.degrees(math.atan2(u_kt, v_kt)) + 360) % 360
            tas_east = gs_east - u_kt
            tas_north = gs_north - v_kt
            tas = math.hypot(tas_east, tas_north)

            tas_list.append(tas)
            u_list.append(u_kt)
            v_list.append(v_kt)
            wind_speed_list.append(wind_speed)
            wind_direction_list.append(wind_direction)
        except Exception:
            tas_list.append(np.nan)
            u_list.append(np.nan)
            v_list.append(np.nan)
            wind_speed_list.append(np.nan)
            wind_direction_list.append(np.nan)

    df['wind_u_kt'] = u_list
    df['wind_v_kt'] = v_list
    df['wind_speed_kt'] = wind_speed_list
    df['wind_direction_deg'] = wind_direction_list
    df['TAS_kt'] = tas_list


    # ensure output columns exist and format utc_time
    df_out_cols = ["utc_time", "time", "latitude", "longitude", "altitude", "ground_speed", "track", "wind_u_kt", "wind_v_kt", "wind_speed_kt", "wind_direction_deg", "TAS_kt", "Temperature_K"]
    for col in df_out_cols:
        if col not in df.columns:
            df[col] = np.nan

    if "utc_time" in df.columns and (df["utc_time"].isna().all() or df["utc_time"].dtype == object and df["utc_time"].eq('').all()) and "time" in df.columns:
        try:
            df["utc_time"] = pd.to_datetime(df["time"], unit='s', utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            df["utc_time"] = ''

    # coerce numeric types; fill wind/TAS missing values with 0 but keep lat/lon/alt as NaN if missing
    numeric_cols_no_fill = ["time", "latitude", "longitude", "altitude", "ground_speed", "track"]
    for nc in numeric_cols_no_fill:
        if nc in df.columns:
            df[nc] = pd.to_numeric(df[nc], errors="coerce")

    numeric_fill_cols = ["wind_u_kt", "wind_v_kt", "wind_speed_kt", "TAS_kt"]
    for nc in numeric_fill_cols:
        if nc in df.columns:
            df[nc] = pd.to_numeric(df[nc], errors="coerce").fillna(0.0)

    # wind direction: fill missing with 0
    if 'wind_direction_deg' in df.columns:
        df['wind_direction_deg'] = pd.to_numeric(df['wind_direction_deg'], errors='coerce').fillna(0.0)

    # ensure 'UTC' column exists for downstream modules that expect it
    if 'UTC' not in df.columns:
        if 'utc_time' in df.columns:
            df['UTC'] = df['utc_time']
        else:
            # try to format from time if available
            try:
                df['UTC'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                df['UTC'] = ''

    # close dataset to release resources
    # Before closing the dataset, attempt to sample a sea-level temperature from ERA5
    # for the first row and compute per-row Temperature_K = T0 - L * h (h in meters).
    try:
        temp0 = None
        L_lapse = 0.0065  # K/m
        if ds is not None and len(df) > 0:
            first = df.iloc[0]
            lat0 = float(first['latitude'])
            lon0 = float(first['longitude'])
            time0 = int(first['time'])
            # helper to extract temperature depending on dataset/source
            def _sample_temperature(lat, lon, t_unix):
                t_dt = pd.to_datetime(t_unix, unit='s', utc=True)
                try:
                    if source == 'era5':
                        # pressure-level ERA5: variable often 't' (Kelvin) with level dim
                        var_candidates = [v for v in ds.variables if v.lower().startswith(('t', 'air_temperature'))]
                        var_name = None
                        for v in ['t', 'temperature', 'air_temperature']:
                            if v in ds.variables:
                                var_name = v
                                break
                        if var_name is None and var_candidates:
                            var_name = var_candidates[0]

                        # sea-level target ~ 1013.25 hPa
                        p_target = 1013.25
                        level_dims = [dim for dim in ds.dims if dim in ['level', 'pressure', 'pressure_level', 'isobaricInhPa']]
                        lon_ = ((lon + 180) % 360) - 180
                        time_sel = ds.sel(time=t_dt, method='nearest')

                        # Try pressure-level sampling first (nearest available level)
                        if level_dims:
                            level_name = level_dims[0]
                            try:
                                level_vals = time_sel[level_name].values if level_name in time_sel.coords else ds[level_name].values
                                level_arr = np.array(level_vals, dtype=float)
                                idx = int(np.abs(level_arr - p_target).argmin())
                                nearest_p = float(level_arr[idx])
                                try:
                                    point = time_sel[var_name].sel({level_name: nearest_p, 'latitude': lat, 'longitude': lon_}, method='nearest')
                                    return float(point.values)
                                except Exception:
                                    # try with p_target directly
                                    try:
                                        point = time_sel[var_name].sel({level_name: p_target, 'latitude': lat, 'longitude': lon_}, method='nearest')
                                        return float(point.values)
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                        # If pressure-level sampling failed or not present, try a set of likely surface variables
                        for surf in ('t2m', '2m_temperature', 'air_temperature', 'surface_temperature', 'temperature'):
                            if surf in ds.variables:
                                try:
                                    point = time_sel[surf].sel({'latitude': lat, 'longitude': lon_}, method='nearest')
                                    return float(point.values)
                                except Exception:
                                    # try small lat/lon offsets in case of grid misalignment
                                    for dlat, dlon in ((0.0,0.0),(0.125,0.0),(-0.125,0.0),(0.0,0.125),(0.0,-0.125)):
                                        try:
                                            point = time_sel[surf].sel({'latitude': lat + dlat, 'longitude': lon_ + dlon}, method='nearest')
                                            return float(point.values)
                                        except Exception:
                                            continue

                        # Finally, if we still have a variable name from candidates try it at nearest lat/lon with small offsets
                        if var_name is not None and var_name in ds.variables:
                            for dlat, dlon in ((0.0,0.0),(0.125,0.0),(-0.125,0.0),(0.0,0.125),(0.0,-0.125)):
                                try:
                                    point = time_sel[var_name].sel({'latitude': lat + dlat, 'longitude': lon_ + dlon}, method='nearest')
                                    return float(point.values)
                                except Exception:
                                    continue

                        return None

                    if source in ('era5_single',):
                        # single-level ERA5: prefer 2m temperature variables
                        for cand in ['t2m', '2m_temperature', 'air_temperature', 'surface_temperature']:
                            if cand in ds.variables:
                                time_sel = ds.sel(time=t_dt, method='nearest')
                                lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
                                lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
                                lon_ = ((lon + 180) % 360) - 180
                                try:
                                    point = time_sel[cand].sel({lat_name: lat, lon_name: lon_}, method='nearest')
                                except Exception:
                                    point = time_sel[cand].sel({lat_name: lat, lon_name: lon}, method='nearest')
                                return float(point.values)
                        return None

                    # GFS fallback: try common temp vars (may be in K)
                    if source.startswith('gfs'):
                        for cand in ['t', 'tmp', 'air_temperature']:
                            if cand in ds.variables:
                                time_sel = ds.sel(time=t_dt, method='nearest')
                                lat_name = 'lat' if 'lat' in ds.coords or 'lat' in ds.dims else 'latitude'
                                lon_name = 'lon' if 'lon' in ds.coords or 'lon' in ds.dims else 'longitude'
                                try:
                                    point = time_sel[cand].sel({lat_name: lat, lon_name: lon}, method='nearest')
                                    return float(point.values)
                                except Exception:
                                    continue
                        return None
                except Exception:
                    return None

            try:
                sampled = _sample_temperature(lat0, lon0, time0)
                temp0 = float(sampled) if sampled is not None else None
            except Exception:
                temp0 = None

            # If we couldn't sample from ERA5/GFS, try Meteostat (station observations)
            if temp0 is None:
                try:
                    try:
                        from meteostat import Stations, Hourly
                    except Exception as _e:
                        # meteostat not installed or import failure
                        raise

                    # round to nearest hour for station hourly data
                    t0 = pd.to_datetime(time0, unit='s', utc=True)
                    tstart = t0.floor('H')
                    tend = tstart

                    stations = Stations().nearby(lat0, lon0).fetch(1)
                    if not stations.empty:
                        st_id = stations.index[0]
                        hr = Hourly(st_id, tstart, tend)
                        df_hr = hr.fetch()
                        if not df_hr.empty and 'temp' in df_hr.columns:
                            temp_c = df_hr['temp'].iloc[0]
                            if pd.notna(temp_c):
                                temp0 = float(temp_c) + 273.15
                                print(f"multi_tas: sampled temp0={temp0} K from Meteostat station {st_id}")
                except ImportError:
                    # meteostat not available in environment; skip
                    pass
                except Exception:
                    # non-fatal: leave temp0 as None
                    pass

            # Debug / diagnostic logging about temperature sampling
            try:
                if temp0 is not None:
                    print(f"multi_tas: sampled temp0={temp0} K from source={source} at lat={lat0} lon={lon0} time={pd.to_datetime(time0, unit='s', utc=True)}")
                else:
                    # collect some dataset diagnostics for debugging
                    try:
                        vars_list = list(ds.variables.keys())
                    except Exception:
                        vars_list = []
                    try:
                        coords_list = list(ds.coords.keys())
                    except Exception:
                        coords_list = []
                    try:
                        dims_list = list(ds.dims.keys())
                    except Exception:
                        dims_list = []
                    print(f"multi_tas: temperature sampling failed (source={source}). ds.vars={vars_list[:20]} coords={coords_list} dims={dims_list}")
            except Exception:
                pass

        # compute per-row Temperature_K column
        try:
            alt_ft = pd.to_numeric(df.get('altitude'), errors='coerce').fillna(0.0)
            alt_m = alt_ft * 0.3048
            if temp0 is None:
                # if we couldn't sample temperature, fall back to ISA sea-level temp (warmer default)
                isa_temp = 303.15
                print(f"multi_tas: falling back to ISA temp0={isa_temp} K for Temperature_K computation")
                df['Temperature_K'] = isa_temp - (L_lapse * alt_m)
            else:
                # temp0 assumed in Kelvin; apply lapse rate
                df['Temperature_K'] = temp0 - (L_lapse * alt_m)
        except Exception:
            df['Temperature_K'] = pd.NA
    except Exception:
        # non-fatal; ensure column exists
        try:
            df['Temperature_K'] = pd.NA
        except Exception:
            pass

    try:
        ds.close()
    except Exception:
        pass

    # Preserve any existing `aircraft_type` column from the original dataframe
    # so downstream modules (which expect this metadata) continue to work.
    result = df.loc[:, df_out_cols].copy()
    if "aircraft_type" in df.columns:
        try:
            result["aircraft_type"] = df["aircraft_type"].values
        except Exception:
            # fallback: copy as-is (may align by index)
            result["aircraft_type"] = df["aircraft_type"]

    return result


__all__ = ["compute_tas_for_dataframe"]
