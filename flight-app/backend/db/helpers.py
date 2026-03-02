"""
Insert and query helpers for flight_run, flight_track, flight_segment.
All timestamps in UTC (ISO format).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .schema import get_connection, init_schema, DB_PATH


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_schema(conn: sqlite3.Connection | None = None) -> None:
    if conn is None:
        with get_connection() as c:
            init_schema(c)
    else:
        init_schema(conn)


def insert_run(
    aircraft_type: str,
    output_csv_path: str,
    etow_kg: float | None = None,
    total_fuel_kg: float | None = None,
    trip_fuel_kg: float | None = None,
    total_co2_kg: float | None = None,
    run_id: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> str:
    rid = run_id or str(uuid4())
    created = _utc_now()
    if conn is None:
        conn = get_connection()
        try:
            _insert_run_impl(conn, rid, aircraft_type, created, etow_kg, total_fuel_kg, trip_fuel_kg, total_co2_kg, output_csv_path)
            conn.commit()
        finally:
            conn.close()
    else:
        _insert_run_impl(conn, rid, aircraft_type, created, etow_kg, total_fuel_kg, trip_fuel_kg, total_co2_kg, output_csv_path)
    return rid


def _insert_run_impl(
    conn: sqlite3.Connection,
    run_id: str,
    aircraft_type: str,
    created_at: str,
    etow_kg: float | None,
    total_fuel_kg: float | None,
    trip_fuel_kg: float | None,
    total_co2_kg: float | None,
    output_csv_path: str,
) -> None:
    conn.execute(
        """INSERT INTO flight_run (run_id, aircraft_type, created_at, etow_kg, total_fuel_kg, trip_fuel_kg, total_co2_kg, output_csv_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, aircraft_type, created_at, etow_kg, total_fuel_kg, trip_fuel_kg, total_co2_kg, output_csv_path),
    )


def insert_track_rows(
    run_id: str,
    rows: list[dict[str, Any]],
    conn: sqlite3.Connection | None = None,
) -> None:
    """Each row: timestamp, latitude, longitude, altitude?, speed?, flight_phase?"""
    if not rows:
        return
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    try:
        conn.executemany(
            """INSERT INTO flight_track (run_id, timestamp, latitude, longitude, altitude, speed, flight_phase)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    run_id,
                    r["timestamp"],
                    r["latitude"],
                    r["longitude"],
                    r.get("altitude"),
                    r.get("speed"),
                    r.get("flight_phase"),
                )
                for r in rows
            ],
        )
        if close:
            conn.commit()
    finally:
        if close:
            conn.close()


def insert_segment_rows(
    run_id: str,
    rows: list[dict[str, Any]],
    conn: sqlite3.Connection | None = None,
) -> None:
    """Each row: timestamp, delta_t_s?, fuel_kg?, co2_kg?"""
    if not rows:
        return
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    try:
        conn.executemany(
            """INSERT INTO flight_segment (run_id, timestamp, delta_t_s, fuel_kg, co2_kg)
               VALUES (?, ?, ?, ?, ?)""",
            [
                (
                    run_id,
                    r["timestamp"],
                    r.get("delta_t_s"),
                    r.get("fuel_kg"),
                    r.get("co2_kg"),
                )
                for r in rows
            ],
        )
        if close:
            conn.commit()
    finally:
        if close:
            conn.close()


def get_run(run_id: str, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    if conn is None:
        conn = get_connection()
        try:
            return _get_run_impl(conn, run_id)
        finally:
            conn.close()
    return _get_run_impl(conn, run_id)


def _get_run_impl(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT run_id, aircraft_type, created_at, etow_kg, total_fuel_kg, trip_fuel_kg, total_co2_kg, output_csv_path FROM flight_run WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def get_track(run_id: str, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    if conn is None:
        conn = get_connection()
        try:
            return _get_track_impl(conn, run_id)
        finally:
            conn.close()
    return _get_track_impl(conn, run_id)


def _get_track_impl(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT timestamp, latitude, longitude, altitude, speed, flight_phase FROM flight_track WHERE run_id = ? ORDER BY timestamp",
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_segments(run_id: str, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    if conn is None:
        conn = get_connection()
        try:
            return _get_segments_impl(conn, run_id)
        finally:
            conn.close()
    return _get_segments_impl(conn, run_id)


def _get_segments_impl(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT timestamp, delta_t_s, fuel_kg, co2_kg FROM flight_segment WHERE run_id = ? ORDER BY timestamp",
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]
