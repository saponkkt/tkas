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

from multi_tas import compute_tas_for_dataframe
from flight_phase import detect_flight_phase
import variable_mass
import thrust as thrust_mod
import Fuel as fuel_mod


def _process_file(input_path: str, output_path: str, compute_tas: bool = False, aircraft_type: Optional[str] = None) -> None:
    """Process a single CSV file and write output CSV."""
    df = pd.read_csv(input_path)

    # Basic case-insensitive normalization for commonly used column names
    # without overwriting existing normalized names.
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
            df = compute_tas_for_dataframe(df, input_csv=input_path)
            print("Computed TAS and wind columns from model data")
        except Exception as e:
            print(f"Error: compute_tas_for_dataframe failed: {e}")
            print("Skipping this file because TAS/wind data are required for downstream calculations.")
            return

    # 3. Ensure UTC column exists for downstream modules (variable_mass expects 'UTC')
    if "UTC" not in df.columns:
        if "utc_time" in df.columns:
            df["UTC"] = df["utc_time"]
        else:
            # if we have epoch seconds in `time`, format to ISO
            try:
                df["UTC"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                df["UTC"] = ""

    # 3. Detect flight phases
    try:
        df = detect_flight_phase(df)
        print("Added column: flight_phase")
    except Exception as e:
        print(f"Warning: detect_flight_phase failed: {e}")

    # 3. Add UTC split columns (delta_t, sum_t, etc.)
    try:
        df = variable_mass.add_utc_split_columns(df, utc_col="UTC")
        print("Added time split columns (delta_t (s), sum_t (s), sum_t (min))")
    except Exception as e:
        print(f"Warning: add_utc_split_columns failed: {e}")

    # 4. Compute thrust (will try to detect phase if missing)
    try:
        thrust_series = thrust_mod.compute_thrust_N(df, type_col="aircraft_type")
        df["Thrust_N"] = thrust_series
        print("Computed column: Thrust_N")
    except Exception as e:
        print(f"Warning: compute_thrust_N failed: {e}")

    # 5. Compute fuel columns
    try:
        # ensure intermediate fuel columns (use aircraft_type column from pipeline)
        df = fuel_mod.add_fnom_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fmin_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fcr_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fapld_column(df, type_col="aircraft_type")
        # add main fuel rate and fuel-at-time
        df = fuel_mod.add_fuel_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fuel_at_time_column(df, type_col="aircraft_type")
        df = fuel_mod.add_fuel_sum_with_time_column(df, type_col="aircraft_type")
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

    # 6. Save
    # Ensure `aircraft_type` column exists in the output (may be provided interactively)
    if "aircraft_type" not in df.columns:
        # If user supplied aircraft_type earlier, use it; otherwise create empty column
        df["aircraft_type"] = aircraft_type if aircraft_type is not None else ""
        print(f"Ensured aircraft_type column in output (set to '{df['aircraft_type'].iloc[0]}' for rows)")

    df.to_csv(output_path, index=False)
    print(f"Saved output to: {output_path}")


def process(input_path: str, output_path: str, compute_tas: bool = True, aircraft_type: Optional[str] = None) -> None:
    """Process either a single CSV file or all CSVs in an input directory.

    If `input_path` is a directory, `output_path` must be a directory. Each
    CSV file in `input_path` will be processed and written to `output_path`
    preserving the filename.
    """
    in_path = Path(input_path)
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
    argv = argv if argv is not None else sys.argv[1:]
    # support: python process_adsb_pipeline.py input.csv output.csv [aircraft_type]
    if len(argv) >= 2:
        input_path, output_path = argv[0], argv[1]
        # If the third CLI arg (aircraft_type) was provided, use it.
        # Otherwise, prompt the user interactively so they can enter
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

    process(input_path, output_path, aircraft_type=aircraft_type)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
