import json
import sys
import glob
import pandas as pd
import os

backend_dir = os.path.dirname(__file__)
cfg_path = os.path.join(backend_dir, 'config.json')

print('CONFIG_PATH:', cfg_path)
try:
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    print('CONFIG_KEYS_COUNT:', len(cfg.keys()))
    print('CONFIG_KEYS_SAMPLE:', list(cfg.keys())[:50])
except Exception as e:
    print('CONFIG_LOAD_ERROR:', e)

pattern = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), '**', '*.csv')
print('SEARCH_PATTERN:', pattern)
files = glob.glob(pattern, recursive=True)
print('FOUND_CSV_COUNT:', len(files))
if not files:
    sys.exit(0)

sample = files[0]
print('SAMPLE_CSV:', sample)
try:
    df = pd.read_csv(sample)
    print('SAMPLE_COLUMNS:', list(df.columns)[:50])
    if 'aircraft_type' in df.columns:
        vals = df['aircraft_type'].dropna().unique()[:20]
        print('UNIQUE_aircraft_type_COUNT:', len(vals))
        print('UNIQUE_aircraft_type_SAMPLE:', list(vals)[:20])
    else:
        # Case-insensitive search
        lower_map = {c.lower(): c for c in df.columns}
        if 'aircraft_type' in lower_map:
            col = lower_map['aircraft_type']
            vals = df[col].dropna().unique()[:20]
            print('FOUND aircraft_type AS:', col)
            print('UNIQUE_aircraft_type_SAMPLE:', list(vals)[:20])
        else:
            print('NO aircraft_type column found in sample CSV')
except Exception as e:
    print('CSV_READ_ERROR:', e)
