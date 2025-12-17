from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from flight_phase import detect_flight_phase


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect flight phases from an ADS-B CSV file.",
    )
    parser.add_argument("input_csv", help="path to input CSV file")
    parser.add_argument(
        "--output",
        "-o",
        help=(
            "path to output CSV file (default: add '_with_phases' before extension)"
        ),
    )
    parser.add_argument(
        "--alt-col",
        default="altitude",
        help="name of altitude column in the CSV (default: 'altitude')",
    )
    parser.add_argument(
        "--track-col",
        default="track",
        help=(
            "name of track/heading column in the CSV (default: 'track'; "
            "if not found and 'Direction' exists, that will be used automatically)"
        ),
    )

    args = parser.parse_args()

    in_path = Path(args.input_csv)
    if not in_path.is_file():
        raise SystemExit(f"Input CSV not found: {in_path}")

    out_path = Path(args.output) if args.output else in_path.with_name(
        f"{in_path.stem}_with_phases{in_path.suffix}"
    )

    df = pd.read_csv(in_path)

    # ถ้าคอลัมน์ที่ระบุไม่มี แต่มี 'Direction' ให้ใช้แทนอัตโนมัติ
    alt_col = args.alt_col
    track_col = args.track_col
    if alt_col not in df.columns:
        # ลองหาคอลัมน์ที่น่าจะเป็น altitude แทน
        for cand in ["Altitude", "alt", "Alt"]:
            if cand in df.columns:
                alt_col = cand
                break
    if track_col not in df.columns and "Direction" in df.columns:
        track_col = "Direction"

    df_out = detect_flight_phase(df, alt_col=alt_col, track_col=track_col)
    df_out.to_csv(out_path, index=False)

    # ใช้ข้อความธรรมดาเพื่อหลีกเลี่ยงปัญหา encoding บน Windows console
    print(f"Flight phases written to: {out_path}")


if __name__ == "__main__":  # pragma: no cover
    main()

