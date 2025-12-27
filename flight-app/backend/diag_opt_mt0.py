import pandas as pd
import numpy as np
import Mass as mass_mod

path = r"C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\LGS\1 sep\SL788_3c009538.csv"
print('Loading', path)
df = pd.read_csv(path)

# Ensure required columns
try:
    df = mass_mod.add_P1_column(df)
    df = mass_mod.add_P2_column(df)
    df = mass_mod.add_P3_column(df)
    df = mass_mod.add_mt_column(df)
    df = mass_mod.add_f2_column(df)
except Exception as e:
    print('Error preparing df:', e)

# Run optimization (force grid fallback for reproducibility)
try:
    df_opt, res = mass_mod.optimize_mt0(df, use_scipy=False)
    print('optimize_mt0 result:', res)
except Exception as e:
    print('optimize_mt0 failed:', e)
    res = {'mt0': None}

# helper to compute objective for an mt0
def compute_obj_for_mt0(df_base, mt0):
    alt = pd.to_numeric(df_base.get('altitude'), errors='coerce')
    fuel = pd.to_numeric(df_base.get('Fuel_at_time_kg'), errors='coerce').fillna(0).astype(float)
    p1 = pd.to_numeric(df_base.get('P1'), errors='coerce').astype(float)
    p2 = pd.to_numeric(df_base.get('P2'), errors='coerce').astype(float)
    p3 = pd.to_numeric(df_base.get('P3'), errors='coerce').astype(float)
    phases = df_base.get('flight_phase')

    mask_cross = alt >= 10000.0
    if not mask_cross.any():
        return float('inf')
    pos0 = int(np.where(mask_cross.values)[0][0])
    n = len(df_base)
    mt = np.full(n, np.nan, dtype=float)
    mt[pos0] = float(mt0)
    for i in range(pos0 - 1, -1, -1):
        mt[i] = mt[i + 1] + float(fuel.iloc[i + 1])
    for i in range(pos0 + 1, n):
        mt[i] = mt[i - 1] - float(fuel.iloc[i])

    f2 = (p1.values * (mt ** 2)) + (p2.values * mt) + p3.values
    phase_mask = (phases == 'Climb') if phases is not None else pd.Series([False] * n, index=df_base.index)
    sel_mask = (alt >= 10000.0) & (alt <= 20000.0) & phase_mask
    selected = f2[sel_mask.values]
    selected = selected[~np.isnan(selected)]
    if selected.size == 0:
        return float('inf')
    return float(np.sum(selected ** 2))

# Excel mt0 reported by you
excel_mt0 = 61298.72
code_mt0 = res.get('mt0')
print('Excel mt0:', excel_mt0)
print('Code mt0 (opt result):', code_mt0)

try:
    obj_excel = compute_obj_for_mt0(df, excel_mt0)
    print('Objective at Excel mt0:', obj_excel)
except Exception as e:
    print('Failed computing objective at Excel mt0:', e)

try:
    if code_mt0 is not None:
        obj_code = compute_obj_for_mt0(df, code_mt0)
        print('Objective at code mt0:', obj_code)
except Exception as e:
    print('Failed computing objective at code mt0:', e)

# show selected rows count and the pos0, sample rows
alt = pd.to_numeric(df.get('altitude'), errors='coerce')
phases = df.get('flight_phase')
mask_sel = (alt >= 10000.0) & (alt <= 20000.0) & (phases == 'Climb')
idx = df.index[mask_sel].tolist()
print('Selected rows count:', len(idx))
if len(idx) > 0:
    print('Selected rows index range (1-based):', idx[0] + 1, 'to', idx[-1] + 1)
    print(df.loc[idx[:20], ['altitude','flight_phase','P1','P2','P3','f2']].to_string(index=False))

# Save optimized df if available
if res.get('mt0') is not None:
    outpath = path.replace('.csv', '.opt.csv')
    df_opt.to_csv(outpath, index=False)
    print('Saved optimized CSV to', outpath)
else:
    print('No optimized result to save')
