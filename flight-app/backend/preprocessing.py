import pandas as pd
import numpy as np
from datetime import timedelta
import json
import glob
import os

# =============================================================================
#  1. CONFIGURATION
# =============================================================================
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')


def _load_preprocessing_config(aircraft_type='737', config_path=DEFAULT_CONFIG_PATH):
    # Fallbacks preserve previous Boeing behavior if config is missing or incomplete.
    default_measurement = {
        'kt_to_ms': 0.51444,
        'ft_to_m': 0.3048,
        'threshold_alt': 100,
    }
    default_aircraft = {
        'roc_avg': 34.36525,
        'rod_avg': -25.96369,
        'takeoff_roll_time': 29.5,
        'landing_roll_time': 30.4,
        'liftoff_spd': 156.58747,
        'touchdown_spd': 142,
        'taxi_spd': 15,
    }

    config = {}
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

    measurement = {**default_measurement, **config.get('measurement', {})}

    aircraft_key = str(aircraft_type) if aircraft_type is not None else '737'
    aircraft_cfg = config.get(aircraft_key, config.get('737', {}))
    aircraft = {**default_aircraft, **aircraft_cfg}

    ft_to_m = float(measurement['ft_to_m'])
    return {
        'KT_TO_MS': float(measurement['kt_to_ms']),
        'FT_TO_M': ft_to_m,
        'THRESHOLD_ALT': float(measurement['threshold_alt']),
        'ROC_AVG': float(aircraft['roc_avg']) * ft_to_m,
        'ROD_AVG': float(aircraft['rod_avg']) * ft_to_m,
        'TAKEOFF_ROLL_TIME': int(round(float(aircraft['takeoff_roll_time']))),
        'LANDING_ROLL_TIME': int(round(float(aircraft['landing_roll_time']))),
        'LIFTOFF_SPD': float(aircraft['liftoff_spd']),
        'TOUCHDOWN_SPD': float(aircraft['touchdown_spd']),
        'TAXI_SPD': float(aircraft['taxi_spd']),
    }

# =============================================================================
#  2. HELPER FUNCTIONS
# =============================================================================

def _parse_position_column(df):
    df.columns = df.columns.str.strip()
    if 'Position' in df.columns:
        try:
            temp = df['Position'].astype(str).str.split(',', expand=True)
            if temp.shape[1] >= 2:
                df['Latitude'] = temp[0].astype(float)
                df['Longitude'] = temp[1].astype(float)
        except Exception as e:
            print(f"   [Error] Parsing Position failed: {e}")
    return df

def _preprocess_adsb_data(df):
    print(f"   [Preprocess] Resampling (Cubic=Lat/Lon, Circular=Direction, Linear=Others)...")

    if 'UTC_datetime' not in df.columns:
        df['UTC_datetime'] = pd.to_datetime(df['UTC'], errors='coerce')

    df = df.dropna(subset=['UTC_datetime'])
    df = df.set_index('UTC_datetime')
    df = df[~df.index.duplicated(keep='first')]

    # Pandas 2.x expects lower-case offset aliases (e.g. "1s").
    df_resampled = df.resample('1s').asfreq()

    pos_cols = [c for c in ['Latitude', 'Longitude'] if c in df_resampled.columns]
    dir_col = 'Direction' if 'Direction' in df_resampled.columns else ('Heading' if 'Heading' in df_resampled.columns else None)
    other_num_cols = [c for c in df_resampled.select_dtypes(include=[np.number]).columns
                      if c not in pos_cols and c != dir_col]
    obj_cols = df_resampled.select_dtypes(exclude=[np.number]).columns

    # 1. Position (Cubic Spline)
    if len(df) > 3:
        try:
            df_resampled[pos_cols] = df_resampled[pos_cols].interpolate(method='cubic')
        except:
            df_resampled[pos_cols] = df_resampled[pos_cols].interpolate(method='linear')
    else:
        df_resampled[pos_cols] = df_resampled[pos_cols].interpolate(method='linear')

    # 2. Direction (Circular Interpolation)
    if dir_col:
        s = df_resampled[dir_col]
        valid_mask = s.notna()
        if valid_mask.sum() > 1:
            valid_rads = np.deg2rad(s[valid_mask].values)
            unwrapped_rads = np.unwrap(valid_rads)

            s_unwrapped = pd.Series(index=s.index, dtype=float)
            s_unwrapped.loc[valid_mask] = unwrapped_rads
            s_unwrapped_interp = s_unwrapped.interpolate(method='slinear')

            df_resampled[dir_col] = np.round(np.rad2deg(s_unwrapped_interp) % 360, 2)
        else:
            df_resampled[dir_col] = s.ffill().bfill()

    # 3. Other Numerics (Linear)
    if len(other_num_cols) > 0:
        df_resampled[other_num_cols] = df_resampled[other_num_cols].interpolate(method='slinear')

    # 4. Objects (Forward Fill)
    if len(obj_cols) > 0:
        df_resampled[obj_cols] = df_resampled[obj_cols].ffill()

    return df_resampled.reset_index()

def _generate_takeoff_phase(ref_row, original_cols, cfg):
    ref_time = ref_row['UTC_datetime']
    ref_alt_m = ref_row['Altitude'] * cfg['FT_TO_M']
    ref_spd_ms = ref_row['Speed'] * cfg['KT_TO_MS']

    constant_lat = ref_row['Latitude']
    constant_lon = ref_row['Longitude']

    cols_to_exclude = ['UTC', 'Timestamp', 'Altitude', 'Speed', 'UTC_datetime', 'Position', 'Latitude', 'Longitude']
    static_data = {c: ref_row[c] for c in original_cols if c not in cols_to_exclude}

    generated_data = []
    curr_time = ref_time

    time_climb = int(ref_alt_m / cfg['ROC_AVG'])
    for t in range(time_climb):
        curr_time -= timedelta(seconds=1)
        h_now = cfg['ROC_AVG'] * (time_climb - t - 1)
        progress = (time_climb - t - 1) / max(1, time_climb)
        v_diff = ref_spd_ms - (cfg['LIFTOFF_SPD'] * cfg['KT_TO_MS'])
        v_now = (cfg['LIFTOFF_SPD'] * cfg['KT_TO_MS']) + (v_diff * progress)

        row = static_data.copy()
        row.update({
            'UTC_datetime': curr_time, 'Altitude': max(0, round(h_now / cfg['FT_TO_M'])),
            'Speed': round(v_now / cfg['KT_TO_MS']), 'Latitude': constant_lat, 'Longitude': constant_lon
        })
        generated_data.append(row)

    curr_time_ground = generated_data[-1]['UTC_datetime'] if generated_data else ref_time
    accel = (cfg['LIFTOFF_SPD'] * cfg['KT_TO_MS']) / cfg['TAKEOFF_ROLL_TIME']
    for t in range(cfg['TAKEOFF_ROLL_TIME']):
        curr_time_ground -= timedelta(seconds=1)
        v_now = (cfg['LIFTOFF_SPD'] * cfg['KT_TO_MS']) - (accel * (t+1))

        row = static_data.copy()
        row.update({
            'UTC_datetime': curr_time_ground, 'Altitude': 0,
            'Speed': max(0, round(v_now / cfg['KT_TO_MS'])), 'Latitude': constant_lat, 'Longitude': constant_lon
        })
        generated_data.append(row)

    generated_data.reverse()
    return pd.DataFrame(generated_data)

def _generate_landing_phase(ref_row, original_cols, cfg):
    ref_time = ref_row['UTC_datetime']
    ref_alt_m = ref_row['Altitude'] * cfg['FT_TO_M']
    ref_spd_ms = ref_row['Speed'] * cfg['KT_TO_MS']

    constant_lat = ref_row['Latitude']
    constant_lon = ref_row['Longitude']

    cols_to_exclude = ['UTC', 'Timestamp', 'Altitude', 'Speed', 'UTC_datetime', 'Position', 'Latitude', 'Longitude']
    static_data = {c: ref_row[c] for c in original_cols if c not in cols_to_exclude}

    generated_data = []
    curr_time = ref_time

    time_desc = int(ref_alt_m / abs(cfg['ROD_AVG']))
    for t in range(time_desc):
        curr_time += timedelta(seconds=1)
        h_now = ref_alt_m - (abs(cfg['ROD_AVG']) * (t + 1))
        progress = (t + 1) / max(1, time_desc)
        v_diff = ref_spd_ms - (cfg['TOUCHDOWN_SPD'] * cfg['KT_TO_MS'])
        v_now = ref_spd_ms - (v_diff * progress)

        row = static_data.copy()
        row.update({
            'UTC_datetime': curr_time, 'Altitude': max(0, round(h_now / cfg['FT_TO_M'])),
            'Speed': round(v_now / cfg['KT_TO_MS']), 'Latitude': constant_lat, 'Longitude': constant_lon
        })
        generated_data.append(row)

    curr_time_ground = generated_data[-1]['UTC_datetime'] if generated_data else ref_time

    speed_start = cfg['TOUCHDOWN_SPD'] * cfg['KT_TO_MS']
    speed_target = cfg['TAXI_SPD'] * cfg['KT_TO_MS']
    speed_diff = speed_start - speed_target

    for t in range(cfg['LANDING_ROLL_TIME']):
        curr_time_ground += timedelta(seconds=1)

        progress = (t + 1) / cfg['LANDING_ROLL_TIME']
        v_now = speed_start - (speed_diff * progress)

        row = static_data.copy()
        row.update({
            'UTC_datetime': curr_time_ground, 'Altitude': 0,
            'Speed': round(v_now / cfg['KT_TO_MS']),
            'Latitude': constant_lat, 'Longitude': constant_lon
        })
        generated_data.append(row)

    return pd.DataFrame(generated_data)

# =============================================================================
#  3. MAIN BATCH EXECUTION
# =============================================================================

def preprocessing(filename: str, aircraft_type: str = '737', config_path: str = DEFAULT_CONFIG_PATH):
    try:
        cfg = _load_preprocessing_config(aircraft_type=aircraft_type, config_path=config_path)

        df = pd.read_csv(filename)
        original_cols = df.columns.tolist()
        df = _parse_position_column(df)

        df['UTC_datetime'] = pd.to_datetime(df['UTC'], errors='coerce')
        df = _preprocess_adsb_data(df)

        start_row = df.iloc[0]
        df_head = pd.DataFrame()
        if start_row['Altitude'] > cfg['THRESHOLD_ALT']:
            df_head = _generate_takeoff_phase(start_row, original_cols, cfg)

        end_row = df.iloc[-1]
        df_tail = pd.DataFrame()
        if end_row['Altitude'] > cfg['THRESHOLD_ALT']:
            df_tail = _generate_landing_phase(end_row, original_cols, cfg)

        df_final = pd.concat([df_head, df, df_tail], ignore_index=True)

        df_final['Timestamp'] = df_final['UTC_datetime'].astype('int64') // 10**9
        df_final['UTC'] = df_final['UTC_datetime'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        if 'Position' in original_cols:
            df_final['Position'] = df_final['Latitude'].map('{:.6f}'.format) + ',' + df_final['Longitude'].map('{:.6f}'.format)

        valid_cols = [c for c in original_cols if c in df_final.columns]
        df_export = df_final[valid_cols].copy()

        output_file = filename.replace('.csv', '_preprocessed.csv')
        df_export.to_csv(output_file, index=False)
        print(f"   ---> ✅ Saved: {output_file}")
    except Exception as e:
        print(f"   ---> ❌ Error with {filename}: {e}")