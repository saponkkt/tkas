"""Temperature Sensitivity Pipeline

This pipeline processes ADS-B CSV with customized Temperature_K values to analyze
temperature sensitivity impacts on TAS calculations.

Usage:
    python temp_sensitivity_pipeline.py input_folder output_folder sensitivity_value aircraft_type

Example:
    python temp_sensitivity_pipeline.py ./input ./output 5 737

Parameters:
    input_folder: Path to folder containing input CSV files
    output_folder: Path to folder for output CSV files  
    sensitivity_value: Temperature sensitivity in Kelvin (int/float)
                      - Sets first N rows to (273.15 + sensitivity_value) K
                      - For remaining rows: T0 - (0.0065 * altitude_m)
                      Where T0 = 273.15 + sensitivity_value
    aircraft_type: Aircraft type code (e.g., '737', 'A320')

The script processes temperature sensitivity by:
1. Loading CSV files from input_folder
2. Computing TAS with normal ERA5 temperature
3. Overriding Temperature_K with custom sensitivity values
4. Running downstream calculations (Mass, Thrust, Fuel)
5. Writing results to output_folder
"""

from __future__ import annotations

import sys
from typing import Optional
import os
import glob
from pathlib import Path
import time

import pandas as pd
import numpy as np

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
if 'Mass_sent' in sys.modules:
    del sys.modules['Mass_sent']

from flight_phase import detect_flight_phase
import variable_mass
import thrust as thrust_mod
import Fuel as fuel_mod
import Mass as mass_mod
import importlib.util


def apply_temperature_sensitivity(
    df: pd.DataFrame, 
    sensitivity_value: float,
    num_rows_override: Optional[int] = None
) -> pd.DataFrame:
    """Apply temperature sensitivity modification to DataFrame.
    
    For the first N rows (where N = sensitivity_value or num_rows_override):
        Temperature_K = 273.15 + sensitivity_value
    
    For remaining rows, apply lapse rate:
        Temperature_K = T0 - (0.0065 * altitude_m)
        Where T0 = 273.15 + sensitivity_value
    
    Args:
        df: DataFrame with 'altitude' and 'Temperature_K' columns
        sensitivity_value: Temperature offset in Kelvin
        num_rows_override: Override number of rows to apply constant temperature (default: use sensitivity_value)
    
    Returns:
        DataFrame with modified Temperature_K column
    """
    df = df.copy()
    
    # Base temperature: 273.15 K (0°C) + sensitivity value
    T0 = 273.15 + sensitivity_value
    
    # Number of rows to apply constant temperature (only first row)
    if num_rows_override is not None:
        n_rows = num_rows_override
    else:
        n_rows = 1  # Only the first row uses constant temperature
    
    # Ensure we have altitude column
    if 'altitude' not in df.columns:
        print("Warning: 'altitude' column not found, using raw sensitivity value for all rows")
        df['Temperature_K'] = T0
        return df
    
    # Convert altitude to numeric (feet)
    alt_ft = pd.to_numeric(df['altitude'], errors='coerce').fillna(0.0)
    
    # Convert feet to meters: multiply by 0.3048
    alt_m = alt_ft * 0.3048
    
    # Lapse rate: 0.0065 K/m
    L_lapse = 0.0065
    
    # Initialize Temperature_K column
    temp_k = np.zeros(len(df))
    
    # First N rows: constant temperature T0
    temp_k[:n_rows] = T0
    
    # Remaining rows: apply lapse rate formula
    # Temperature_K = T0 - (L_lapse * altitude_m)
    if len(df) > n_rows:
        temp_k[n_rows:] = T0 - (L_lapse * alt_m[n_rows:])
    
    df['Temperature_K'] = temp_k
    
    print(f"Applied temperature sensitivity: T0={T0:.2f}K, "
          f"constant for first {n_rows} rows, "
          f"then lapse rate {L_lapse} K/m for remaining {len(df)-n_rows} rows")
    
    return df


def _process_file(
    input_path: str, 
    output_path: str, 
    compute_tas: bool = False, 
    aircraft_type: Optional[str] = None, 
    sensitivity_value: Optional[float] = None
) -> None:
    """Process a single CSV file with temperature sensitivity.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to output CSV file
        compute_tas: Whether to compute TAS from wind data
        aircraft_type: Aircraft type for computations
        sensitivity_value: Temperature sensitivity value in Kelvin
    """
    df = pd.read_csv(input_path)
    
    print(f"\n{'='*80}")
    print(f"Processing: {input_path}")
    print(f"{'='*80}")

    # Basic case-insensitive normalization for commonly used column names
    existing = {c.lower(): c for c in df.columns}
    if "altitude" not in df.columns:
        if "altitude" in existing:
            df["altitude"] = df[existing["altitude"]]
        elif "alt" in existing:
            df["altitude"] = df[existing["alt"]]
        elif "altitude_ft" in existing:
            df["altitude"] = df[existing["altitude_ft"]]

    # If user provided an aircraft type, set it on every row
    if aircraft_type is not None:
        df["aircraft_type"] = str(aircraft_type)
        print(f"Applied aircraft_type='{aircraft_type}' to all rows")

    # Ensure we have a UTC column for variable_mass
    if "UTC" not in df.columns and "timestamp" in df.columns:
        df["UTC"] = df["timestamp"].astype(str)

    # Compute TAS using model winds (GFS / ERA5)
    if compute_tas:
        try:
            t0 = time.time()
            df_new = compute_tas_for_dataframe(df, input_csv=input_path)
            t1 = time.time()
            print(f"Timing: compute_tas_for_dataframe took {t1-t0:.2f}s")
            if len(df_new) == 0:
                print("Warning: compute_tas returned empty DataFrame, skipping TAS computation")
            else:
                df = df_new
                print("Computed TAS and wind columns from model data")
        except Exception as e:
            print(f"Error: compute_tas_for_dataframe failed: {e}")
            print("Skipping this file because TAS/wind data are required for downstream calculations.")
            return

    # ===== TEMPERATURE SENSITIVITY MODIFICATION =====
    # Apply custom temperature sensitivity instead of using ERA5 temperatures
    if sensitivity_value is not None:
        try:
            df = apply_temperature_sensitivity(df, sensitivity_value)
        except Exception as e:
            print(f"Error applying temperature sensitivity: {e}")
            print("Continuing with original Temperature_K values")
    # ===== END TEMPERATURE SENSITIVITY =====

    # Ensure UTC column exists for downstream modules
    if "UTC" not in df.columns:
        if "utc_time" in df.columns:
            utc_val = df["utc_time"]
            if isinstance(utc_val, pd.DataFrame):
                try:
                    utc_series = utc_val.apply(lambda row: next((x for x in row if pd.notnull(x)), None), axis=1)
                except Exception:
                    utc_series = utc_val.iloc[:, 0]
            else:
                utc_series = utc_val

            try:
                df["UTC"] = pd.to_datetime(utc_series, errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ").fillna("")
            except Exception:
                df["UTC"] = utc_series.astype(str).fillna("")
        else:
            try:
                df["UTC"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                df["UTC"] = ""

    # Detect flight phases
    try:
        try:
            if "ROCD_m/s" not in df.columns:
                if "UTC" in df.columns and "altitude" in df.columns:
                    dt = pd.to_datetime(df["UTC"], errors="coerce")
                    dt_s = dt.diff().dt.total_seconds().fillna(1.0)
                    alt = pd.to_numeric(df["altitude"], errors="coerce")
                    try:
                        max_alt = float(alt.max(skipna=True))
                    except Exception:
                        max_alt = None
                    if max_alt is not None and max_alt > 2500:
                        alt_m = alt * 0.3048
                    else:
                        alt_m = alt
                    rocd_ms = alt_m.diff().fillna(0) / dt_s.replace(0, 1.0)
                    rocd_ms_clean = np.round(rocd_ms.fillna(0), decimals=1)
                    df["ROCD_m/s"] = rocd_ms_clean
                    print("Computed ROCD_m/s from UTC and altitude")
        except Exception as _e:
            print(f"Warning: could not compute ROCD_m/s: {_e}")

        t0 = time.time()
        df = detect_flight_phase(df)
        t1 = time.time()
        print(f"Timing: detect_flight_phase took {t1-t0:.2f}s")
        print("Added column: flight_phase")
    except Exception as e:
        print(f"Warning: detect_flight_phase failed: {e}")

    # Add UTC split columns (delta_t, sum_t, etc.)
    try:
        t0 = time.time()
        df = variable_mass.add_utc_split_columns(df, utc_col="UTC")
        t1 = time.time()
        print(f"Timing: add_utc_split_columns took {t1-t0:.2f}s")
        print("Added time split columns (delta_t (s), sum_t (s), sum_t (min))")
    except Exception as e:
        print(f"Warning: add_utc_split_columns failed: {e}")

    # Compute thrust (will try to detect phase if missing)
    try:
        t0 = time.time()
        thrust_series = thrust_mod.compute_thrust_N(df, type_col="aircraft_type")
        df["Thrust_N"] = thrust_series
        t1 = time.time()
        print(f"Timing: compute_thrust_N took {t1-t0:.2f}s")
        print("Computed column: Thrust_N")
    except Exception as e:
        print(f"Warning: compute_thrust_N failed: {e}")

    # Compute fuel columns
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
    except Exception as e:
        print(f"Warning: fuel computation failed: {e}")

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

    # Compute mass-related columns from Mass.py (P1,P2,P3,mt,f2,Sumsq)
    try:
        df = mass_mod.add_P1_column(df)
        df = mass_mod.add_P2_column(df)
        df = mass_mod.add_P3_column(df)
        # First pass: use a baseline mt_offset to initialize
        df = mass_mod.add_mt_column(df, mt_offset=57796.0)
        df = mass_mod.add_f2_column(df)
        df = mass_mod.add_sumsq_column(df)
        print("Computed mass-related columns: P1, P2, P3, mt, f2, Sumsq (initial)")
    except Exception as e:
        print(f"Warning: mass computation failed: {e}")

    # Optional: optimize mt0 to minimize Sumsq
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

    # Compute Total_Energy columns if available (CL, CD, D, Thrust_N_TE)
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
                    dt = df["delta_t (s)"].astype(float).replace({0: np.nan})
                    rocd_ms = df["altitude"].astype(float).diff().fillna(0) / dt.ffill().fillna(1)
                    df["ROCD_m/s"] = rocd_ms.fillna(0)
                    print("Computed ROCD_m/s from altitude and delta_t (s)")

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
            else:
                print("Fuel&CO2_TE module not available; skipping Fuel/CO2 TE columns")
        except Exception as e:
            print(f"Warning: Fuel&CO2_TE integration failed: {e}")

    except Exception:
        print("Warning: Total_Energy module not found; skipping TE columns")

    # Ensure `aircraft_type` column exists in the output (may be provided interactively)
    if "aircraft_type" not in df.columns:
        # If user supplied aircraft_type earlier, use it; otherwise create empty column
        df["aircraft_type"] = aircraft_type if aircraft_type is not None else ""
        print(f"Ensured aircraft_type column in output")

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

    # Write output
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✓ Saved output to: {output_path}")
    except Exception as e:
        print(f"Error writing output: {e}")
        raise

    # Add summary rows at the end of CSV
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


def _process_directory(
    input_dir: str,
    output_dir: str,
    compute_tas: bool = False,
    aircraft_type: Optional[str] = None,
    sensitivity_value: Optional[float] = None
) -> None:
    """Process all CSV files in a directory.
    
    Args:
        input_dir: Path to directory with input CSV files
        output_dir: Path to directory for output CSV files
        compute_tas: Whether to compute TAS from wind data
        aircraft_type: Aircraft type for computations
        sensitivity_value: Temperature sensitivity value in Kelvin
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    
    if not input_dir.exists():
        print(f"Error: input directory does not exist: {input_dir}")
        return
    
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        print(f"Warning: no CSV files found in {input_dir}")
        return
    
    print(f"\nProcessing {len(csv_files)} CSV files from {input_dir}")
    print(f"Temperature sensitivity: {sensitivity_value} K")
    print(f"Aircraft type: {aircraft_type}")
    print(f"Output directory: {output_dir}\n")
    
    success_count = 0
    error_count = 0
    
    for input_file in csv_files:
        try:
            output_file = output_dir / input_file.name
            _process_file(
                str(input_file),
                str(output_file),
                compute_tas=compute_tas,
                aircraft_type=aircraft_type,
                sensitivity_value=sensitivity_value
            )
            success_count += 1
        except Exception as e:
            print(f"✗ Error processing {input_file.name}: {e}")
            error_count += 1
    
    print(f"\n{'='*80}")
    print(f"Processing complete: {success_count} successful, {error_count} errors")
    print(f"{'='*80}\n")


def main():
    """Main entry point for temperature sensitivity pipeline."""
    if len(sys.argv) < 5:
        print(__doc__)
        print("Error: Missing required arguments")
        print("Usage: python temp_sensitivity_pipeline.py <input_folder> <output_folder> <sensitivity_value> <aircraft_type>")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    
    try:
        sensitivity_value = float(sys.argv[3])
    except ValueError:
        print(f"Error: sensitivity_value must be a number, got: {sys.argv[3]}")
        sys.exit(1)
    
    aircraft_type = sys.argv[4]
    
    # Process directory
    _process_directory(
        input_folder,
        output_folder,
        compute_tas=True,  # Always compute TAS
        aircraft_type=aircraft_type,
        sensitivity_value=sensitivity_value
    )


if __name__ == "__main__":
    main()
