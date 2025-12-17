from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from thrust import (
    compute_thr_max_climb_ISA,
    compute_delta_temp,
    compute_delta_temp_eff,
    compute_thr_max_climb,
    compute_thrust_N,
)


def _add_derived_columns(df: pd.DataFrame, altitude_col: str) -> pd.DataFrame:
    """Normalize/derive columns needed for thrust calculations.

    - Ensure numeric `altitude` column exists (ft) from `altitude_col`.
    - If `temperature_K` is missing, approximate ISA temperature profile.
    """
    out = df.copy()

    # 1) altitude -> create numeric `altitude` column in feet
    if altitude_col not in out.columns:
        raise ValueError(f"Altitude column '{altitude_col}' not found in input CSV")

    out["altitude"] = pd.to_numeric(out[altitude_col], errors="coerce")

    # 2) approximate temperature if not present (ISA with sea-level 288.15 K)
    if "temperature_K" not in out.columns:
        L = 0.0065  # K/m
        alt_m = out["altitude"] * 0.3048
        out["temperature_K"] = 288.15 - (L * alt_m)

    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute thrust-related columns (Thr_max_climb_ISA, delta_temp, "
            "delta_temp_eff, Thr_max_climb, Thrust_N) from a CSV with flight phases."
        ),
    )
    parser.add_argument("input_csv", help="path to input CSV file (must include flight_phase)")
    parser.add_argument(
        "--output",
        "-o",
        help=(
            "path to output CSV file (default: add 'results_with_thrust' or "
            "'_with_thrust' before extension)"
        ),
    )
    parser.add_argument(
        "--alt-col",
        default="Altitude",
        help="name of altitude column in the CSV (default: 'Altitude')",
    )
    parser.add_argument(
        "--type-col",
        default="aircraft_type",
        help=(
            "name of column specifying aircraft type (default: 'aircraft_type'); "
            "if missing, --aircraft-type will be used for all rows"
        ),
    )
    parser.add_argument(
        "--aircraft-type",
        default=None,
        help=(
            "fallback aircraft type key (e.g. '737' or '320') to apply to all "
            "rows when type column is missing"
        ),
    )

    args = parser.parse_args()

    in_path = Path(args.input_csv)
    if not in_path.is_file():
        raise SystemExit(f"Input CSV not found: {in_path}")

    # Default output: <stem>_with_thrust<suffix>
    if args.output:
        out_path = Path(args.output)
    else:
        out_path = in_path.with_name(f"{in_path.stem}_with_thrust{in_path.suffix}")

    df = pd.read_csv(in_path)

    if "flight_phase" not in df.columns:
        raise SystemExit("Input CSV must contain a 'flight_phase' column.")

    # Ensure type column exists; if not and a fixed type is given, create it
    type_col = args.type_col
    if type_col not in df.columns:
        if args.aircraft_type is None:
            raise SystemExit(
                "No aircraft type information found. Please either provide a "
                f"column '{type_col}' in the CSV or use --aircraft-type."
            )
        df[type_col] = args.aircraft_type

    # Derive altitude/temperature columns as required by thrust helpers
    df_for_calc = _add_derived_columns(df, altitude_col=args.alt_col)

    # Compute each component
    thr_max_isa = compute_thr_max_climb_ISA(df_for_calc, type_col=type_col)
    delta_temp = compute_delta_temp(df_for_calc, temp_col="temperature_K")
    delta_temp_eff = compute_delta_temp_eff(
        df_for_calc, type_col=type_col, temp_col="temperature_K"
    )
    thr_max = compute_thr_max_climb(
        df_for_calc, type_col=type_col, temp_col="temperature_K"
    )
    thrust_N = compute_thrust_N(
        df_for_calc,
        type_col=type_col,
        phase_col="flight_phase",
        alt_col="altitude",
        temp_col="temperature_K",
    )

    # Attach to original DataFrame for output
    df_out = df.copy()
    df_out["Thr_max_climb_ISA"] = thr_max_isa
    df_out["delta_temp"] = delta_temp
    df_out["delta_temp_eff"] = delta_temp_eff
    df_out["Thr_max_climb"] = thr_max
    df_out["Thrust_N"] = thrust_N

    df_out.to_csv(out_path, index=False)
    # ใช้ข้อความธรรมดาเพื่อหลีกเลี่ยงปัญหา encoding บน Windows console
    print(f"Thrust results written to: {out_path}")


if __name__ == "__main__":  # pragma: no cover
    main()

