import argparse
import pandas as pd
from backend.flight_phase import detect_flight_phase


def run(csv_path: str, out_path: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # auto-detect altitude and track column names (case-insensitive)
    alt_col = None
    track_col = None
    for c in df.columns:
        cl = c.lower()
        if alt_col is None and ('alt' in cl or 'height' in cl):
            alt_col = c
        if track_col is None and (cl == 'direction' or 'track' in cl or 'heading' in cl):
            track_col = c

    # fallback names
    if alt_col is None:
        alt_col = 'altitude'
    if track_col is None:
        track_col = 'Direction'

    df_out = detect_flight_phase(df, alt_col=alt_col, track_col=track_col)
    if out_path:
        df_out.to_csv(out_path, index=False)
    return df_out


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run flight phase detection on a CSV file')
    parser.add_argument('csv', nargs='?', default='backend/tests/sample_adsb.csv', help='Path to input CSV')
    parser.add_argument('--out', '-o', default='backend/tests/results_attached.csv', help='Path to write results CSV')
    args = parser.parse_args()

    df_out = run(args.csv, out_path=args.out)
    print(df_out[['Timestamp', 'UTC', 'Callsign', 'altitude', 'Speed', 'Direction', 'flight_phase']].to_string(index=False))
