"""
gs_check.py
-----------
Verify Ground Speed accuracy by reversing the TAS equation:

  Forward:  TAS⃗ = GS⃗ − Wind⃗
  Reverse:  GS_CHECK⃗ = TAS⃗ + Wind⃗

Usage (Git Bash / Terminal):
    python gs_check.py path/to/flight_data.csv

Output:
    <original_filename>_gs_check.csv  (saved in the same directory as input)
"""

import os
import sys
import math
import numpy as np
import pandas as pd

# Import TAS computation function from multi_tas.py (must be in same directory)
try:
    from multi_tas import compute_tas_for_dataframe
except ImportError:
    print("ERROR: Cannot import multi_tas.py — make sure it is in the same directory as gs_check.py")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def compute_gs_check(df_tas: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame that already has TAS_kt, wind_u_kt, wind_v_kt, track columns,
    compute GS_CHECK by vector addition:  GS_CHECK⃗ = TAS⃗ + Wind⃗

    Parameters
    ----------
    df_tas : pd.DataFrame
        Output from compute_tas_for_dataframe(); must contain:
        TAS_kt, wind_u_kt, wind_v_kt, track (or Direction after normalization)

    Returns
    -------
    pd.DataFrame with GS_CHECK, %error_signed, %error_absolute columns added.
    """
    gs_check_list = []

    for _, row in df_tas.iterrows():
        try:
            tas_kt = float(row["TAS_kt"])
            u_kt   = float(row["wind_u_kt"])   # eastward wind (knots)
            v_kt   = float(row["wind_v_kt"])   # northward wind (knots)
            trk    = float(row["track"])        # track angle in degrees

            if not all(np.isfinite([tas_kt, u_kt, v_kt, trk])):
                raise ValueError("non-finite value")

            # Reconstruct TAS vector from magnitude + track angle
            track_rad  = math.radians(trk)
            tas_north  = tas_kt * math.cos(track_rad)
            tas_east   = tas_kt * math.sin(track_rad)

            # Add wind back to get GS_CHECK vector
            gs_check_north = tas_north + v_kt
            gs_check_east  = tas_east  + u_kt

            # GS_CHECK scalar magnitude
            gs_check = math.hypot(gs_check_east, gs_check_north)
            gs_check_list.append(gs_check)

        except Exception:
            gs_check_list.append(np.nan)

    df_tas["GS_CHECK"] = gs_check_list
    return df_tas


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # ── 1. Parse CLI argument ──────────────────────────────────────────────
    if len(sys.argv) < 2:
        print("Usage: python gs_check.py <path_to_csv>")
        sys.exit(1)

    input_path = sys.argv[1]

    if not os.path.isfile(input_path):
        print(f"ERROR: File not found → {input_path}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  gs_check.py  |  Input: {os.path.basename(input_path)}")
    print(f"{'='*60}")

    # ── 2. Load CSV ────────────────────────────────────────────────────────
    print("\n[1/5] Loading CSV ...")
    try:
        df_raw = pd.read_csv(input_path, sep=None, engine="python")
    except Exception as e:
        print(f"ERROR: Failed to read CSV — {e}")
        sys.exit(1)

    print(f"      Rows loaded : {len(df_raw)}")
    print(f"      Columns     : {list(df_raw.columns)}")

    # Keep original Speed and Callsign before passing to multi_tas
    # (multi_tas renames 'Speed' → 'ground_speed' internally)
    speed_original = None
    for col in df_raw.columns:
        if col.strip().lower() == "speed":
            speed_original = df_raw[col].copy()
            break

    if speed_original is None:
        print("ERROR: No 'Speed' column found in CSV.")
        sys.exit(1)

    callsign_col = None
    for col in df_raw.columns:
        if col.strip().lower() == "callsign":
            callsign_col = df_raw[col].copy()
            break

    # ── 3. Compute TAS via multi_tas ──────────────────────────────────────
    print("\n[2/5] Computing TAS and wind components via multi_tas.py ...")
    try:
        df_tas = compute_tas_for_dataframe(df_raw.copy())
    except Exception as e:
        print(f"ERROR: compute_tas_for_dataframe() failed — {e}")
        sys.exit(1)

    print(f"      TAS computed for {df_tas['TAS_kt'].notna().sum()} / {len(df_tas)} rows")

    # ── 4. Compute GS_CHECK ───────────────────────────────────────────────
    print("\n[3/5] Computing GS_CHECK (vector: TAS⃗ + Wind⃗) ...")
    df_tas = compute_gs_check(df_tas)
    print(f"      GS_CHECK computed for {df_tas['GS_CHECK'].notna().sum()} / {len(df_tas)} rows")

    # ── 5. Attach original Speed and compute %error ───────────────────────
    print("\n[4/5] Calculating %error ...")

    # Re-attach original Speed values aligned by index
    df_tas["Speed_original"] = speed_original.values if len(speed_original) == len(df_tas) else np.nan
    df_tas["Speed_original"] = pd.to_numeric(df_tas["Speed_original"], errors="coerce")

    # %error_signed   = ((GS_CHECK − Speed) / Speed) × 100
    # %error_absolute = |%error_signed|
    df_tas["%error_signed"] = np.where(
        df_tas["Speed_original"] == 0,
        np.nan,
        (df_tas["GS_CHECK"] - df_tas["Speed_original"]) / df_tas["Speed_original"]
    ) * 100

    df_tas["%error_absolute"] = df_tas["%error_signed"].abs()

    # ── 6. Build output DataFrame ─────────────────────────────────────────
    print("\n[5/5] Building output CSV ...")

    # Ensure utc_time column exists
    if "utc_time" not in df_tas.columns and "UTC" in df_tas.columns:
        df_tas["utc_time"] = df_tas["UTC"]

    # Build output with required columns
    output_cols = {
        "utc_time"        : df_tas.get("utc_time",   pd.Series([np.nan]*len(df_tas))),
        "Callsign"        : callsign_col.values if callsign_col is not None else np.nan,
        "latitude"        : df_tas.get("latitude",   pd.Series([np.nan]*len(df_tas))),
        "longitude"       : df_tas.get("longitude",  pd.Series([np.nan]*len(df_tas))),
        "altitude"        : df_tas.get("altitude",   pd.Series([np.nan]*len(df_tas))),
        "Speed"           : df_tas["Speed_original"],
        "TAS_kt"          : df_tas["TAS_kt"],
        "GS_CHECK"        : df_tas["GS_CHECK"].round(4),
        "%error_signed"   : df_tas["%error_signed"].round(4),
        "%error_absolute" : df_tas["%error_absolute"].round(4),
    }

    df_out = pd.DataFrame(output_cols, index=df_tas.index)

    # ── 7. Save output CSV ────────────────────────────────────────────────
    input_dir      = os.path.dirname(os.path.abspath(input_path))
    base_name      = os.path.splitext(os.path.basename(input_path))[0]
    output_filename = f"{base_name}_gs_check.csv"
    output_path    = os.path.join(input_dir, output_filename)

    df_out.to_csv(output_path, index=False)

    # ── 8. Summary ────────────────────────────────────────────────────────
    valid = df_out["%error_absolute"].dropna()

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    print(f"  Total rows processed    : {len(df_out)}")
    print(f"  Rows with valid result  : {len(valid)}")
    print(f"  Mean |%error|           : {valid.mean():.4f} %")
    print(f"  Max  |%error|           : {valid.max():.4f} %")
    print(f"  Min  |%error|           : {valid.min():.4f} %")
    print(f"\n  Output saved to:")
    print(f"  → {output_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()