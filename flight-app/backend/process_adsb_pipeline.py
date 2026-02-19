"""Small pipeline to process ADS-B CSV using project modules.

Usage:
    python process_adsb_pipeline.py input.csv output.csv

The script will attempt to run the following steps in order:
 - basic validation via `TAS_Temp._ensure_required_columns`
 - add `flight_phase` via `flight_phase.detect_flight_phase`
 - add time + delta columns via `variable_mass.add_utc_split_columns`
 - compute thrust via `thrust.compute_thrust_N`
 - compute fuel columns via `Fuel.add_fuel_column` and related helpers

Helper scripts:
 - `add_aircraft_type.py`: CLI helper to add an `aircraft_type` column to CSV files (file or directory).

The script is defensive: it will continue when optional inputs are missing.
"""

from __future__ import annotations

import sys
from typing import Optional
import os
import glob
from pathlib import Path

import pandas as pd
import numpy as np
import time

from multi_tas import compute_tas_for_dataframe
import sys
# Clear any cached imports to ensure latest code is used
if 'flight_phase' in sys.modules:
    del sys.modules['flight_phase']
if 'variable_mass' in sys.modules:
    del sys.modules['variable_mass']
if 'thrust' in sys.modules:
    del sys.modules['thrust']
if 'Fuel' in sys.modules:
    del sys.modules['Fuel']
if 'Mass' in sys.modules:
    del sys.modules['Mass']

from flight_phase import detect_flight_phase

# Verify we're using the correct flight_phase module
import flight_phase as fp_module
print(f"flight_phase module file: {fp_module.__file__}")
import inspect
source_lines = inspect.getsource(fp_module.detect_flight_phase)
if "rocd_val_case3" in source_lines:
    print("OK: Using FIXED flight_phase.py (has rocd_val_case3 post-processing)")
else:
    print("WARNING: Using OLD flight_phase.py (missing rocd_val_case3 post-processing)")

import variable_mass
import thrust as thrust_mod
import Fuel as fuel_mod
import Mass as mass_mod
import importlib.util


print("Script loaded")

def _process_file(input_path: str, output_path: str, compute_tas: bool = True, aircraft_type: Optional[str] = None) -> None:
    """Process a single CSV file and write output CSV."""
    df = pd.read_csv(input_path)

    # Parse Position into latitude and longitude if present
    if 'Position' in df.columns:
        try:
            df[['latitude', 'longitude']] = df['Position'].str.split(',', expand=True).astype(float)
            print("Parsed Position into latitude and longitude")
        except Exception as e:
            print(f"Warning: could not parse Position: {e}")

    # Normalize column names
    rename_dict = {}
    if 'Altitude' in df.columns and 'altitude' not in df.columns:
        rename_dict['Altitude'] = 'altitude'
    if 'Speed' in df.columns and 'TAS_kt' not in df.columns:
        rename_dict['Speed'] = 'TAS_kt'
    if 'Direction' in df.columns and 'track' not in df.columns:
        rename_dict['Direction'] = 'track'
    df.rename(columns=rename_dict, inplace=True)
    if rename_dict:
        print(f"Renamed columns: {rename_dict}")

    # Basic case-insensitive normalization for commonly used column names
    existing = {c.lower(): c for c in df.columns}
    if "altitude" not in df.columns:
        if "altitude" in existing:
            df["altitude"] = df[existing["altitude"]]
        elif "alt" in existing:
            df["altitude"] = df[existing["alt"]]
        elif "altitude_ft" in existing:
            df["altitude"] = df[existing["altitude_ft"]]
    # If Position is provided as a single column like 'Position' or 'position',
    # leave parsing to downstream modules; ensure lowercase latitude/longitude
    # are not overwritten here.

    # If user provided an aircraft type, set it on every row so downstream
    # functions that expect an `aircraft_type` column will pick it up.
    if aircraft_type is not None:
        # Always set/overwrite to ensure downstream modules receive the type.
        df["aircraft_type"] = str(aircraft_type)
        print(f"Applied aircraft_type='{aircraft_type}' to all rows")

    # 1. Basic normalization is handled inside compute_tas_for_dataframe when needed

    # Ensure we have a UTC column for variable_mass (if timestamp exists, copy it)
    if "UTC" not in df.columns and "timestamp" in df.columns:
        df["UTC"] = df["timestamp"].astype(str)

    # 2. Optionally compute TAS using model winds (GFS / ERA5).
    # Run this before flight phase detection so that altitude/lat/lon
    # normalization is available to `detect_flight_phase`.
    if compute_tas:
        try:
            t0 = time.time()
            df_new = compute_tas_for_dataframe(df, input_csv=input_path)
            t1 = time.time()
            print(f"Timing: compute_tas_for_dataframe took {t1-t0:.2f}s")
            if len(df_new) == 0:
                print("Warning: compute_tas returned empty DataFrame, skipping TAS computation")
                # continue without TAS
            else:
                df = df_new
                print("Computed TAS and wind columns from model data")
        except Exception as e:
            print(f"Error: compute_tas_for_dataframe failed: {e}")
            print("Skipping this file because TAS/wind data are required for downstream calculations.")
            return

    # 3. Ensure UTC column exists for downstream modules (variable_mass expects 'UTC')
    if "UTC" not in df.columns:
        if "utc_time" in df.columns:
            utc_val = df["utc_time"]
            # If `utc_time` is a DataFrame (multiple columns), coalesce to a single Series
            if isinstance(utc_val, pd.DataFrame):
                try:
                    # take first non-null value across columns for each row
                    utc_series = utc_val.apply(lambda row: next((x for x in row if pd.notnull(x)), None), axis=1)
                except Exception:
                    # fallback: take the first column
                    utc_series = utc_val.iloc[:, 0]
            else:
                utc_series = utc_val

            # Try to parse to datetime ISO; fallback to string when parsing fails
            try:
                df["UTC"] = pd.to_datetime(utc_series, errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ").fillna("")
            except Exception:
                df["UTC"] = utc_series.astype(str).fillna("")
        else:
            # if we have epoch seconds in `time`, format to ISO
            try:
                df["UTC"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                df["UTC"] = ""

    # 3. Detect flight phases
    try:
        # Ensure ROCD_m/s exists: compute from UTC + altitude when missing
        try:
            if "ROCD_m/s" not in df.columns:
                if "UTC" in df.columns and "altitude" in df.columns:
                    dt = pd.to_datetime(df["UTC"], errors="coerce")
                    dt_s = dt.diff().dt.total_seconds().fillna(1.0)
                    alt = pd.to_numeric(df["altitude"], errors="coerce")
                    # Heuristic: if alt values are large, assume feet and convert to meters
                    try:
                        max_alt = float(alt.max(skipna=True))
                    except Exception:
                        max_alt = None
                    if max_alt is not None and max_alt > 2500:
                        alt_m = alt * 0.3048
                    else:
                        alt_m = alt
                    rocd_ms = alt_m.diff().fillna(0) / dt_s.replace(0, 1.0)
                    # Clean floating-point noise: round to 1 decimal place
                    # This converts values like 3.3e-12 to 0.0
                    rocd_ms_clean = np.round(rocd_ms.fillna(0), decimals=1)
                    df["ROCD_m/s"] = rocd_ms_clean
                    print("Computed ROCD_m/s from UTC and altitude")
                else:
                    print("ROCD_m/s missing and UTC/altitude not available; skipping ROCD computation")
        except Exception as _e:
            print(f"Warning: could not compute ROCD_m/s: {_e}")

        t0 = time.time()
        df = detect_flight_phase(df)
        t1 = time.time()
        print(f"Timing: detect_flight_phase took {t1-t0:.2f}s")
        print("Added column: flight_phase")
        # Debug: Check red rows right after detect_flight_phase
        try:
            red_check = df.iloc[2043-1:2072]
            print(f"DEBUG: Red rows after detect_flight_phase: Cruise={( red_check['flight_phase'] == 'Cruise').sum()}/30")
        except:
            pass
        # Post-processing disabled: detect_flight_phase() now includes Cases 2-4 handling
        # that properly handles Climb at cruise altitude and plateau detection
    except Exception as e:
        print(f"Warning: detect_flight_phase failed: {e}")

    # 3. Add UTC split columns (delta_t, sum_t, etc.)
    try:
        t0 = time.time()
        df = variable_mass.add_utc_split_columns(df, utc_col="UTC")
        t1 = time.time()
        print(f"Timing: add_utc_split_columns took {t1-t0:.2f}s")
        print("Added time split columns (delta_t (s), sum_t (s), sum_t (min))")
    except Exception as e:
        print(f"Warning: add_utc_split_columns failed: {e}")

    # 4. Compute thrust (will try to detect phase if missing)
    try:
        t0 = time.time()
        thrust_series = thrust_mod.compute_thrust_N(df, type_col="aircraft_type")
        df["Thrust_N"] = thrust_series
        t1 = time.time()
        print(f"Timing: compute_thrust_N took {t1-t0:.2f}s")
        print("Computed column: Thrust_N")
    except Exception as e:
        print(f"Warning: compute_thrust_N failed: {e}")

    # 5. Compute fuel columns
    try:
        t0_fuel = time.time()
        # ensure intermediate fuel columns (use aircraft_type column from pipeline)
        df = fuel_mod.add_fnom_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fmin_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fcr_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fapld_column(df, type_col="aircraft_type")
        # add main fuel rate and fuel-at-time
        df = fuel_mod.add_fuel_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fuel_at_time_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fuel_sum_with_time_column(df, type_col="aircraft_type")
        t1_fuel = time.time()
        print(f"Timing: fuel helpers took {t1_fuel-t0_fuel:.2f}s")
        print("Computed fuel columns and cumulative fuel")
        # Diagnostics: print presence/counts of key fuel-related columns for debugging
        key_cols = [
            "Thrust_N",
            "eta_kg_per_min_per_kN",
            "fnom_kg_per_s",
            "fmin_kg_per_s",
            "fcr_kg_per_s",
            "fapld_kg_per_s",
            "Fuel_kg_per_s",
            "Fuel_at_time_kg",
            "Fuel_sum_with_time_kg",
        ]
        present = [c for c in key_cols if c in df.columns]
        if present:
            counts = {c: int(df[c].notna().sum()) for c in present}
            print("Fuel diagnostics - non-null counts:", counts)
            try:
                print(df[present].head(5).to_string(index=False))
            except Exception:
                pass
    except Exception as e:
        import traceback

        print("Warning: fuel computation failed:")
        traceback.print_exc()
    
    

    # Create alias column names expected elsewhere (add _TE suffixes and safe names)
    try:
        # map existing names to expected names
        alias_map = {
            "fnom": "fnom_TE",
            "fmin": "fmin_TE",
            "fap/ld": "fap_ld",
            "fcr": "fcr_TE",
            "CO2_at_time": "CO2_at_time_TE",
            "CO2_sum_with_time": "CO2_sum_with_time_TE",
        }
        for src, dst in alias_map.items():
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src]
                print(f"Aliased column: {src} -> {dst}")
    except Exception as e:
        print(f"Warning: aliasing Fuel&CO2_TE columns failed: {e}")
    # 6. Compute mass-related columns from Mass.py (P1,P2,P3,mt,f2,Sumsq)
    try:
        df = mass_mod.add_P1_column(df)
        df = mass_mod.add_P2_column(df)
        df = mass_mod.add_P3_column(df)
        # First pass: use a baseline mt_offset to initialize
        df = mass_mod.add_mt_column(df, mt_offset=57796.0)
        df = mass_mod.add_f2_column(df)
        df = mass_mod.add_sumsq_column(df)

        print("Computed mass-related columns: P1, P2, P3, mt, f2, Sumsq (initial)")
        mass_cols = ["P1", "P2", "P3", "mt", "f2", "Sumsq"]
        present = [c for c in mass_cols if c in df.columns]
        if present:
            counts = {c: int(df[c].notna().sum()) for c in present}
            print("Mass diagnostics - non-null counts:", counts)
            try:
                print(df[present].head(5).to_string(index=False))
            except Exception:
                pass
    except Exception as e:
        import traceback

        print("Warning: mass computation failed:")
        traceback.print_exc()

    # 6.5 Optional: optimize mt0 to minimize Sumsq
    # Try to use optimizer to find mt value that minimizes aerodynamic objective
    try:
        print(f"Running optimize_mt0 without target (using only aerodynamic objective)...")
        target_mt = None
        weight_aero = 1.0
        weight_target = 0.0
        
        # Save flight_phase before optimization (in case optimize_mt0 doesn't preserve it)
        flight_phase_backup = df['flight_phase'].copy() if 'flight_phase' in df.columns else None
        
        df_opt, result = mass_mod.optimize_mt0(
            df,
            target_mt0=target_mt,
            weight_aero=weight_aero,
            weight_target=weight_target,
            excel_nonneg=True
        )
        if result.get("mt0") is not None:
            opt_mt0 = result["mt0"]
            print(f"Optimized mt0={opt_mt0:.2f}, objective={result.get('objective', 'N/A')}")
            print(f"Optimized mt[0]={df_opt['mt'].iloc[0]:.2f}")
            df = df_opt
            # ALWAYS restore flight_phase from backup (not just if missing)
            if flight_phase_backup is not None:
                df['flight_phase'] = flight_phase_backup
        else:
            print(f"Optimization skipped: {result.get('skipped', 'unknown reason')}")
    except Exception as e:
        print(f"Warning: optimize_mt0 failed: {e}")
        import traceback
        traceback.print_exc()

    # 6.x Compute Total_Energy columns if available (CL, CD, D, Thrust_N_TE)
    try:
        from Total_Energy import add_CL, add_CD, add_D, add_Thrust_N_TE

        try:
            df = add_CL(df)
            print("Added column: CL")
        except Exception as e:
            print(f"Warning: add_CL failed: {e}")

        try:
            df = add_CD(df)
            print("Added column: CD")
        except Exception as e:
            print(f"Warning: add_CD failed: {e}")

        try:
            df = add_D(df)
            print("Added column: D")
        except Exception as e:
            print(f"Warning: add_D failed: {e}")

        try:
            # If ROCD not present, attempt to compute from altitude and delta time
            if "ROCD_m/s" not in df.columns:
                if "altitude" in df.columns and "delta_t (s)" in df.columns:
                    # ROCD_m/s = delta altitude (m) / delta_t (s)
                    # altitude may be in meters already; diff gives meters if so
                    dt = df["delta_t (s)"].astype(float).replace({0: np.nan})
                    # compute altitude diff; if altitude is in feet convert elsewhere — assume meters here
                    rocd_ms = df["altitude"].astype(float).diff().fillna(0) / dt.ffill().fillna(1)
                    df["ROCD_m/s"] = rocd_ms.fillna(0)
                    print("Computed ROCD_m/s from altitude and delta_t (s)")
                else:
                    print("ROCD_m/s column missing and altitude/delta_t not available; skipping ROCD_m/s computation")

            df = add_Thrust_N_TE(df)
            print("Added column: Thrust_N_TE")
        except Exception as e:
            print(f"Warning: add_Thrust_N_TE failed: {e}")

        # Now run Fuel&CO2_TE helpers (requires Thrust_N_TE present)
        try:
            fuel_co2_te = None
            try:
                module_path = Path(__file__).parent / "Fuel&CO2_TE.py"
                spec = importlib.util.spec_from_file_location("fuel_co2_te", str(module_path))
                if spec and spec.loader:
                    fuel_co2_te = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(fuel_co2_te)
            except Exception as e:
                fuel_co2_te = None
                print(f"Warning: could not load Fuel&CO2_TE module: {e}")

            if fuel_co2_te is not None:
                try:
                    df = fuel_co2_te.add_fnom_TE(df)
                    print("Added column: fnom")
                except Exception as e:
                    print(f"Warning: add_fnom_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_fmin_TE(df)
                    print("Added column: fmin")
                except Exception as e:
                    print(f"Warning: add_fmin_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_fap_ld(df)
                    print("Added column: fap/ld")
                except Exception as e:
                    print(f"Warning: add_fap_ld failed: {e}")

                try:
                    df = fuel_co2_te.add_fcr_TE(df)
                    print("Added column: fcr")
                except Exception as e:
                    print(f"Warning: add_fcr_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_Fuel_TE(df)
                    print("Added column: Fuel_TE")
                except Exception as e:
                    print(f"Warning: add_Fuel_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_Fuel_at_time_TE(df)
                    print("Added column: Fuel_at_time_TE")
                except Exception as e:
                    print(f"Warning: add_Fuel_at_time_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_Fuel_sum_with_time_TE(df)
                    print("Added column: Fuel_sum_with_time_TE")
                except Exception as e:
                    print(f"Warning: add_Fuel_sum_with_time_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_CO2_at_time_TE(df)
                    print("Added column: CO2_at_time")
                except Exception as e:
                    print(f"Warning: add_CO2_at_time_TE failed: {e}")

                try:
                    df = fuel_co2_te.add_CO2_sum_with_time_TE(df)
                    print("Added column: CO2_sum_with_time")
                except Exception as e:
                    print(f"Warning: add_CO2_sum_with_time_TE failed: {e}")
                # (aliasing removed) do not create TE-suffixed alias columns here
            else:
                print("Fuel&CO2_TE module not available; skipping Fuel/CO2 TE columns")
        except Exception as e:
            print(f"Warning: Fuel&CO2_TE integration failed: {e}")

    except Exception:
        print("Warning: Total_Energy module not found; skipping TE columns")

    # 6. Save
    # Ensure `aircraft_type` column exists in the output (may be provided interactively)
    if "aircraft_type" not in df.columns:
        # If user supplied aircraft_type earlier, use it; otherwise create empty column
        df["aircraft_type"] = aircraft_type if aircraft_type is not None else ""
        print(f"Ensured aircraft_type column in output (set to '{df['aircraft_type'].iloc[0]}' for rows)")

    # Ensure `flight_phase` exists in output (fallback to avoid missing column)
    if 'flight_phase' not in df.columns:
        df['flight_phase'] = 'Unknown'
        print("Warning: flight_phase missing; added fallback 'Unknown' column before saving")

    # Reorder columns: put flight_phase near the front for visibility
    cols = list(df.columns)
    if 'flight_phase' in cols:
        cols.remove('flight_phase')
        # Insert flight_phase as column 3 (after likely: timestamp, latitude, longitude / altitude, speed)
        insert_pos = min(5, len(cols))  # Insert near the front but not at position 0
        cols.insert(insert_pos, 'flight_phase')
        df = df[cols]
        print(f"Reordered columns: flight_phase moved to position {insert_pos}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved output to: {output_path}")
    print(f"flight_phase column stats: {df['flight_phase'].value_counts().to_dict()}")
    
    # 7. Add summary rows at the end of CSV
    # Extract summary values
    try:
        etow = None
        if 'mt' in df.columns and len(df) > 0:
            if 'flight_phase' in df.columns:
                takeoff_mask = df['flight_phase'] == 'Takeoff'
                takeoff_indices = df[takeoff_mask].index.tolist()
                if takeoff_indices:
                    etow = df.loc[takeoff_indices[0], 'mt']
                else:
                    etow = df['mt'].iloc[0]
            else:
                etow = df['mt'].iloc[0]
        total_fuel = df['Fuel_sum_with_time_TE'].iloc[-1] if 'Fuel_sum_with_time_TE' in df.columns and len(df) > 0 else None
        total_co2 = df['CO2_sum_with_time'].iloc[-1] if 'CO2_sum_with_time' in df.columns and len(df) > 0 else None
        
        # Calculate Trip_fuel from Fuel_at_time_TE between Takeoff and Landing
        trip_fuel = None
        if 'flight_phase' in df.columns and 'Fuel_at_time_TE' in df.columns:
            try:
                # Find first row with Takeoff
                takeoff_mask = df['flight_phase'] == 'Takeoff'
                landing_mask = df['flight_phase'] == 'Landing'
                
                takeoff_indices = df[takeoff_mask].index.tolist()
                landing_indices = df[landing_mask].index.tolist()
                
                if takeoff_indices and landing_indices:
                    takeoff_start = takeoff_indices[0]
                    landing_end = landing_indices[-1]
                    
                    # Extract Fuel_at_time_TE values in the range [Takeoff start, Landing end]
                    trip_fuel_vals = df.loc[takeoff_start:landing_end, 'Fuel_at_time_TE'].astype(float)
                    trip_fuel = trip_fuel_vals.sum()
            except Exception as e:
                print(f"Warning: could not calculate Trip_fuel: {e}")
        
        # Append summary rows to CSV file
        with open(output_path, 'a') as f:
            f.write('\n')  # blank line separator
            if etow is not None:
                f.write(f'ETOW,{etow}\n')
            if total_fuel is not None:
                f.write(f'Total_Fuel,{total_fuel}\n')
            if trip_fuel is not None:
                f.write(f'Trip_fuel,{trip_fuel}\n')
            if total_co2 is not None:
                f.write(f'Total_CO2,{total_co2}\n')
        
        print(f"Added summary: ETOW={etow}, Total_Fuel={total_fuel}, Trip_fuel={trip_fuel}, Total_CO2={total_co2}")
    except Exception as e:
        print(f"Warning: could not add summary rows: {e}")


def process(input_path: str, output_path: str, compute_tas: bool = True, aircraft_type: Optional[str] = None) -> None:
    """Process either a single CSV file or all CSVs in an input directory.

    If `input_path` is a directory, `output_path` must be a directory. Each
    CSV file in `input_path` will be processed and written to `output_path`
    preserving the filename.
    """
    print("Input path:", repr(input_path))
    in_path = Path(input_path)
    print(f"Is dir: {in_path.is_dir()}")
    print(f"Starting process for {input_path}")
    out_path = Path(output_path)

    if in_path.is_dir():
        # Ensure output directory exists
        out_path.mkdir(parents=True, exist_ok=True)

        files = sorted(glob.glob(str(in_path / "*.csv")))
        if not files:
            print(f"No CSV files found in directory: {in_path}")
            return

        for f in files:
            fname = Path(f).name
            dest = out_path / fname
            print(f"Processing {fname} -> {dest}")
            try:
                _process_file(f, str(dest), compute_tas=compute_tas, aircraft_type=aircraft_type)
            except Exception as e:
                print(f"Failed to process {fname}: {e}")
    else:
        # single file
        _process_file(str(in_path), str(out_path), compute_tas=compute_tas, aircraft_type=aircraft_type)


def main(argv: Optional[list] = None) -> int:
    print("Starting main")
    argv = argv if argv is not None else sys.argv[1:]
    print("argv:", argv)
    # support: python process_adsb_pipeline.py input.csv output.csv [aircraft_type]
    if len(argv) >= 3:
        print("len >= 3")
        input_path, output_path = argv[0], argv[1]
        # an `aircraft_type` which will be applied to every row.
        aircraft_type = argv[2] if len(argv) >= 3 else None
        if aircraft_type is None:
            try:
                atype = input("Aircraft type (e.g. 737, 320) [optional]: ").strip()
                aircraft_type = atype if atype != "" else None
            except Exception:
                aircraft_type = None
    
    else:
        # interactive fallback
        try:
            input_path = input("Input CSV path: ").strip()
            output_path = input("Output CSV path: ").strip()
            atype = input("Aircraft type (e.g. 737, 320) [optional]: ").strip()
            aircraft_type = atype if atype != "" else None
        except Exception:
            print("Usage: python process_adsb_pipeline.py input.csv output.csv [aircraft_type]")
            return 2
    
    print("Calling process")
    process(input_path, output_path, compute_tas=True, aircraft_type=aircraft_type)


if __name__ == "__main__":
    main()
