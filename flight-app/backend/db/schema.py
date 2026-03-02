"""
SQLite schema for flight analysis.
Database file: flights.db
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

# Default DB path: same directory as this file, or project root
DB_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DB_DIR / "flights.db"


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection | None = None) -> None:
    if conn is None:
        conn = get_connection()
        try:
            _create_tables(conn)
            conn.commit()
        finally:
            conn.close()
    else:
        _create_tables(conn)


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS flight_run (
            run_id TEXT PRIMARY KEY,
            aircraft_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            etow_kg REAL,
            total_fuel_kg REAL,
            trip_fuel_kg REAL,
            total_co2_kg REAL,
            output_csv_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS flight_track (
            run_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            altitude REAL,
            speed REAL,
            flight_phase TEXT,
            FOREIGN KEY (run_id) REFERENCES flight_run(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_flight_track_run_id ON flight_track(run_id);

        CREATE TABLE IF NOT EXISTS flight_segment (
            run_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            delta_t_s REAL,
            fuel_kg REAL,
            co2_kg REAL,
            FOREIGN KEY (run_id) REFERENCES flight_run(run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_flight_segment_run_id ON flight_segment(run_id);
    """)


if __name__ == "__main__":
    init_schema()
    print("Schema initialized at", DB_PATH)
