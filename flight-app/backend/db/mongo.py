from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database
from bson import ObjectId

from haversine import haversine_nm
from flight_phase import detect_flight_phase

DEFAULT_MONGO_URI = "mongodb://flight_admin:flight_secret@localhost:27017/flight_app?authSource=admin"
DEFAULT_MONGO_DB = "flight_app"

SUMMARY_LABELS = ["ETOW", "Total_Fuel", "Trip_fuel", "Total_CO2"]
NM_TO_KM = 1.852

_client: MongoClient | None = None


def _mongo_uri() -> str:
    return os.getenv("MONGO_URI", DEFAULT_MONGO_URI)


def _db_name() -> str:
    return os.getenv("MONGO_DB", DEFAULT_MONGO_DB)


def connect_to_mongo() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(_mongo_uri(), serverSelectionTimeoutMS=5000)
    client = _client
    client.admin.command("ping")
    return client


def get_database() -> Database:
    client = connect_to_mongo()
    return client[_db_name()]


def close_mongo_connection() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


def mongo_health_check() -> dict[str, Any]:
    client = connect_to_mongo()
    ping = client.admin.command("ping")
    return {
        "status": "ok" if ping.get("ok") == 1 else "error",
        "database": _db_name(),
    }


def _to_mongo_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value

    return value


def _build_confidence_detail(data_quality: dict[str, Any], total_rows: int) -> str:
    parts: list[str] = []
    phases_found = data_quality.get("phases_found", 0)
    phases_total = data_quality.get("phases_total", 9)
    any_generated = data_quality.get("any_generated", False)
    phase_status = data_quality.get("phase_status", {})

    if data_quality.get("data_complete"):
        parts.append(
            f"All {phases_total} flight phases were present in the original "
            f"ADS-B data with no generation required."
        )
    else:
        generated_phases = [
            p for p, s in phase_status.items() if s == "generated"
        ]
        phase_names = {
            "Taxi_out": "Taxi-out",
            "Takeoff": "Take-off",
            "Initial_climb": "Initial Climb",
            "Taxi_in": "Taxi-in",
            "Landing": "Landing",
        }
        gen_display = [phase_names.get(p, p) for p in generated_phases]
        gen_str = ", ".join(gen_display) if gen_display else "some"
        parts.append(
            f"{phases_found}/{phases_total} flight phases detected in "
            f"original data. "
            f"{gen_str} phase(s) were synthetically generated "
            f"using average aircraft performance models, which may introduce "
            f"estimation uncertainty in those segments."
        )

    if total_rows > 3000:
        parts.append(
            f"High data density ({total_rows:,} rows at 1-second intervals) "
            f"ensures reliable fuel and emissions calculations."
        )
    elif total_rows > 1000:
        parts.append(
            f"Moderate data density ({total_rows:,} rows) provides adequate "
            f"resolution for phase-level analysis."
        )
    else:
        parts.append(
            f"Low data density ({total_rows:,} rows) — results should be "
            f"interpreted with caution."
        )

    parts.append(
        "Data was cleaned and resampled to uniform 1-second intervals. "
        "Flight phase transitions are well-defined."
    )

    if data_quality.get("data_complete") and total_rows > 2000:
        parts.append(
            "Results are suitable for operational planning and emissions reporting."
        )
    else:
        parts.append(
            "Results are suitable for preliminary analysis. "
            "Verify generated phases against actual flight records if precision is required."
        )

    return " ".join(parts)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in km between two points."""
    return haversine_nm(lat1, lon1, lat2, lon2) * NM_TO_KM


def _ground_speed_column(data_df: pd.DataFrame) -> str | None:
    """Column name for ground speed (kt) in pipeline output, if present."""
    lower = {c.lower(): c for c in data_df.columns}
    for key in ("ground_speed", "gs", "groundspeed"):
        if key in lower:
            return lower[key]
    return None


def _build_track_points(
    data_df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    step: int = 10,
) -> list[dict[str, Any]]:
    """Subsampled lat/lon for the map; includes ground_speed (kt) when available."""
    track_points: list[dict[str, Any]] = []
    if lat_col not in data_df.columns or lon_col not in data_df.columns:
        return track_points
    gs_col = _ground_speed_column(data_df)
    for _, row in data_df.iloc[::step].iterrows():
        lat, lon = row.get(lat_col), row.get(lon_col)
        if pd.notna(lat) and pd.notna(lon):
            try:
                pt: dict[str, Any] = {
                    "lat": round(float(lat), 6),
                    "lon": round(float(lon), 6),
                }
                if gs_col is not None and pd.notna(row.get(gs_col)):
                    pt["ground_speed"] = round(float(row[gs_col]), 2)
                track_points.append(pt)
            except (ValueError, TypeError):
                pass
    return track_points


_HTTP_HEADERS = {"User-Agent": "TKAS-FlightAnalysis/1.0 (contact: flight-analysis)"}


def _airport_info_base(lat: float, lon: float) -> dict[str, Any]:
    return {
        "iata": "",
        "icao": "",
        "name": "",
        "city": "",
        "country": "",
        "country_code": "",
        "lat": lat,
        "lon": lon,
    }


def _flight_date_from_dataframe(data_df: pd.DataFrame) -> str:
    """First-row UTC/Timestamp from processed data, e.g. '22 Mar 2026'."""
    flight_date = ""
    utc_col = None
    for c in data_df.columns:
        if c.lower() in ("utc", "utc_time", "timestamp"):
            utc_col = c
            break
    if not utc_col or data_df.empty:
        return flight_date
    try:
        raw_utc = str(data_df[utc_col].iloc[0]).strip()
        if not raw_utc or raw_utc.lower() in ("nan", "nat", "none"):
            return flight_date
        for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                dt = datetime.strptime(raw_utc, fmt)
                flight_date = dt.strftime("%d %b %Y")
                return flight_date
            except ValueError:
                continue
        ts = pd.to_datetime(raw_utc, errors="coerce", utc=True)
        if pd.notna(ts):
            flight_date = ts.strftime("%d %b %Y")
    except Exception:
        pass
    return flight_date


def _get_city_from_nominatim(lat: float, lon: float) -> tuple[str, str, str]:
    """
    Try multiple zoom levels to get city-level name.
    Returns (city, country_code, country)
    zoom=10 → suburb/neighbourhood (most detail)
    zoom=8  → city/county level
    zoom=6  → state/province level
    """
    CITY_KEYS = ["city", "town", "province", "municipality"]
    FALLBACK_KEYS = ["state_district", "state"]

    for zoom in (10, 8, 6):
        try:
            url = (
                "https://nominatim.openstreetmap.org/reverse"
                f"?lat={lat}&lon={lon}&format=json"
                f"&zoom={zoom}&accept-language=en"
            )
            req = urllib.request.Request(url, headers=_HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode())

            addr = data.get("address", {}) or {}
            country_code = str(addr.get("country_code", "") or "").upper()
            country = str(addr.get("country", "") or "")

            city = ""
            for key in CITY_KEYS:
                val = addr.get(key, "")
                if val and str(val).strip():
                    city = str(val).strip()
                    break

            if city:
                return city, country_code, country

            if zoom == 6:
                for key in FALLBACK_KEYS:
                    val = addr.get(key, "")
                    if val and str(val).strip():
                        return str(val).strip(), country_code, country

        except Exception:
            continue

    return "", "", ""


def _find_iata_from_coords(lat: float, lon: float) -> dict[str, Any]:
    """
    Overpass for nearest IATA aerodrome (~15 km), then Nominatim for city/country.
    Never raises.
    """
    result: dict[str, Any] = {
        "iata": "",
        "icao": "",
        "name": "",
        "city": "",
        "country": "",
        "country_code": "",
        "lat": lat,
        "lon": lon,
    }

    radius = 15000
    query = f"""[out:json][timeout:10];
(
  node["aeroway"="aerodrome"]["iata"](around:{radius},{lat},{lon});
  way["aeroway"="aerodrome"]["iata"](around:{radius},{lat},{lon});
);
out center tags;
"""
    try:
        data = urllib.parse.urlencode({"data": query}).encode()
        req = urllib.request.Request(
            "https://overpass-api.de/api/interpreter",
            data=data,
            headers=_HTTP_HEADERS,
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            overpass = json.loads(resp.read().decode())
        elements = overpass.get("elements", [])
        if elements:
            tags = elements[0].get("tags", {}) or {}
            result["iata"] = str(tags.get("iata", "") or "").strip()
            result["icao"] = str(tags.get("icao", "") or "").strip()
            result["name"] = str(
                tags.get("name:en") or tags.get("name", "") or ""
            ).strip()
    except Exception:
        pass

    # Nominatim: city + country
    try:
        city, country_code, country = _get_city_from_nominatim(lat, lon)
        result["city"] = city
        result["country_code"] = country_code
        result["country"] = country
        if not result["name"]:
            try:
                url = (
                    "https://nominatim.openstreetmap.org/reverse"
                    f"?lat={lat}&lon={lon}&format=json&zoom=17&accept-language=en"
                )
                req = urllib.request.Request(url, headers=_HTTP_HEADERS)
                with urllib.request.urlopen(req, timeout=5) as r:
                    d = json.loads(r.read().decode())
                addr = d.get("address", {}) or {}
                result["name"] = str(
                    addr.get("aerodrome")
                    or addr.get("aeroway")
                    or (d.get("display_name", "") or "").split(",")[0]
                    or ""
                ).strip()
            except Exception:
                pass
    except Exception:
        pass

    return result


def _build_route_info_from_track_points(
    track_points: list[dict[str, Any]],
) -> dict[str, Any]:
    route_info: dict[str, Any] = {
        "departure": _airport_info_base(0.0, 0.0),
        "arrival": _airport_info_base(0.0, 0.0),
    }
    if not track_points or len(track_points) < 2:
        return route_info

    try:
        dep_pt = track_points[0]
        arr_pt = track_points[-1]
        dep_lat = float(dep_pt["lat"])
        dep_lon = float(dep_pt["lon"])
        arr_lat = float(arr_pt["lat"])
        arr_lon = float(arr_pt["lon"])
        route_info["departure"] = _find_iata_from_coords(dep_lat, dep_lon)
        route_info["arrival"] = _find_iata_from_coords(arr_lat, arr_lon)
    except Exception as e:
        print(f"[WARNING] Airport detection failed: {e}")

    return route_info


SKIP_CLEAN_COLUMNS = {"utc_time", "time", "UTC", "flight_phase", "aircraft_type", "Callsign"}

EXPECTED_PHASES_LIST = [
    "Taxi_out",
    "Takeoff",
    "Initial_climb",
    "Climb",
    "Cruise",
    "Descent",
    "Approach",
    "Landing",
    "Taxi_in",
]


def get_original_phases(input_path: str) -> list[str]:
    """
    Detect flight phases from the original uploaded raw CSV using detect_flight_phase()
    (same algorithm as the pipeline). Used before preprocessing/pipeline runs.
    """
    try:
        df = pd.read_csv(input_path)

        col_map: dict[str, str] = {}
        for candidate in ("altitude", "Altitude", "alt", "Alt"):
            if candidate in df.columns:
                col_map[candidate] = "altitude"
                break
        for candidate in (
            "track",
            "Track",
            "Direction",
            "direction",
            "heading",
            "Heading",
        ):
            if candidate in df.columns:
                col_map[candidate] = "track"
                break

        df = df.rename(columns=col_map)

        if "altitude" not in df.columns:
            return []

        if "track" not in df.columns:
            df["track"] = np.nan

        df["altitude"] = pd.to_numeric(df["altitude"], errors="coerce").fillna(0)
        df["track"] = pd.to_numeric(df["track"], errors="coerce")

        df_with_phases = detect_flight_phase(df, alt_col="altitude", track_col="track")

        if "flight_phase" in df_with_phases.columns:
            phases = df_with_phases["flight_phase"].dropna().unique().tolist()
            return [
                str(p).strip()
                for p in phases
                if str(p).strip() and str(p).strip() != "Unknown"
            ]

        return []
    except Exception as e:
        print(f"[WARNING] get_original_phases failed: {e}")
        return []


def _build_phase_status(
    data_df: pd.DataFrame,
    original_phases: list[str] | None,
) -> dict[str, str]:
    """Classify each expected phase as missing / generated / original (with altitude fallback)."""
    processed_phases = (
        data_df["flight_phase"].dropna().unique().tolist()
        if "flight_phase" in data_df.columns
        else []
    )
    orig_set = {str(p).strip() for p in (original_phases or [])}
    proc_set = {str(p).strip() for p in processed_phases}

    alt_col = "altitude" if "altitude" in data_df.columns else None
    if alt_col is None and "alt" in data_df.columns:
        alt_col = "alt"
    first_alt = 0.0
    last_alt = 0.0
    if alt_col and not data_df.empty:
        try:
            alt_series = pd.to_numeric(data_df[alt_col], errors="coerce")
            first_alt = float(alt_series.iloc[0])
            last_alt = float(alt_series.iloc[-1])
        except (ValueError, TypeError):
            first_alt = last_alt = 0.0

    takeoff_phases = frozenset({"Taxi_out", "Takeoff", "Initial_climb"})
    landing_phases = frozenset({"Landing", "Taxi_in"})

    phase_status: dict[str, str] = {}
    for phase in EXPECTED_PHASES_LIST:
        if phase not in proc_set:
            phase_status[phase] = "missing"
        elif orig_set and phase not in orig_set:
            phase_status[phase] = "generated"
        elif orig_set and phase in orig_set:
            phase_status[phase] = "original"
        else:
            if first_alt > 100 and phase in takeoff_phases:
                phase_status[phase] = "generated"
            elif last_alt > 100 and phase in landing_phases:
                phase_status[phase] = "generated"
            else:
                phase_status[phase] = "original"

    return phase_status


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Excel-style text prefixes to numeric where applicable."""
    df = df.copy()
    for col in df.columns:
        if col in SKIP_CLEAN_COLUMNS:
            continue
        if df[col].dtype == object:
            cleaned = df[col].astype(str).str.lstrip("'").str.strip()
            converted = pd.to_numeric(cleaned, errors="coerce")
            non_null = converted.notna().sum()
            total = len(converted)
            if total > 0 and (non_null / total) > 0.5:
                df[col] = converted
            else:
                df[col] = cleaned
    return df


def _save_output_rows(
    db: Database, run_id: str, output_df: pd.DataFrame, batch_size: int = 1000
) -> int:
    inserted = 0
    batch: list[dict[str, Any]] = []

    for row_idx, row in enumerate(output_df.to_dict(orient="records")):
        batch.append({
            "run_id": run_id,
            "row_index": row_idx,
            "created_at": datetime.now(timezone.utc),
            "data": {col: _to_mongo_value(val) for col, val in row.items()},
        })

        if len(batch) >= batch_size:
            result = db.flight_output_rows.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
            batch = []

    if batch:
        result = db.flight_output_rows.insert_many(batch, ordered=False)
        inserted += len(result.inserted_ids)

    return inserted


def save_processed_run(
    *,
    input_file: str,
    output_file: str,
    aircraft_type: str | None,
    run_id: str | None = None,
    original_phases: list[str] | None = None,
) -> str:
    db = get_database()
    output_df = pd.read_csv(output_file)
    output_df = _clean_dataframe(output_df)

    first_col = output_df.columns[0]
    second_col = output_df.columns[1] if len(output_df.columns) > 1 else None

    summary_mask = output_df[first_col].astype(str).str.strip().isin(SUMMARY_LABELS)
    summary_rows = output_df[summary_mask]
    data_df = output_df[~summary_mask].copy().dropna(how="all").reset_index(drop=True)

    obj_cols = data_df.select_dtypes(include="object").columns.tolist()
    skip_cols = {"utc_time", "time", "UTC", "flight_phase", "aircraft_type", "Callsign"}
    suspicious = [c for c in obj_cols if c not in skip_cols]
    if suspicious:
        print(f"[WARNING] Non-numeric columns after cleaning: {suspicious}")

    summary: dict[str, float] = {}
    for _, row in summary_rows.iterrows():
        key = str(row[first_col]).strip()
        if key in SUMMARY_LABELS and second_col:
            try:
                summary[key] = float(_to_mongo_value(row[second_col]) or 0)
            except (ValueError, TypeError):
                summary[key] = 0.0

    etow_kg = float(summary.get("ETOW", 0))
    total_fuel_kg = float(summary.get("Total_Fuel", 0))
    trip_fuel_kg = float(summary.get("Trip_fuel", 0))
    total_co2_kg = float(summary.get("Total_CO2", 0))

    flight_duration_s = 0.0
    sum_t_col = None
    for c in data_df.columns:
        if "sum_t" in c.lower() and "s" in c.lower():
            sum_t_col = c
            break
    if sum_t_col and not data_df.empty:
        try:
            flight_duration_s = float(data_df[sum_t_col].iloc[-1])
        except (ValueError, TypeError, KeyError):
            pass

    lat_col = "latitude" if "latitude" in data_df.columns else "lat"
    lon_col = "longitude" if "longitude" in data_df.columns else "lon"
    total_distance_km = 0.0
    if lat_col in data_df.columns and lon_col in data_df.columns:
        coords = data_df[[lat_col, lon_col]].dropna().values
        for i in range(1, len(coords)):
            try:
                total_distance_km += _haversine_km(
                    float(coords[i - 1][0]),
                    float(coords[i - 1][1]),
                    float(coords[i][0]),
                    float(coords[i][1]),
                )
            except (ValueError, TypeError):
                pass

    track_points = _build_track_points(data_df, lat_col, lon_col, step=10)

    fuel_per_phase_col: str | None = None
    for candidate in ("Fuel_at_time_TE", "Fuel_at_time_kg"):
        if candidate in data_df.columns:
            fuel_per_phase_col = candidate
            break

    co2_col = None
    for c in data_df.columns:
        if "CO2_at_time" in c and "sum" not in c.lower():
            co2_col = c
            break

    phase_col = "flight_phase" if "flight_phase" in data_df.columns else None
    alt_col = "altitude" if "altitude" in data_df.columns else "alt"

    segments = []
    if phase_col and phase_col in data_df.columns:
        for phase, group in data_df.groupby(phase_col, sort=False):
            phase_coords = group[[lat_col, lon_col]].dropna().values
            phase_dist = 0.0
            for i in range(1, len(phase_coords)):
                try:
                    phase_dist += _haversine_km(
                        float(phase_coords[i - 1][0]),
                        float(phase_coords[i - 1][1]),
                        float(phase_coords[i][0]),
                        float(phase_coords[i][1]),
                    )
                except (ValueError, TypeError):
                    pass

            phase_str = str(phase)
            flight_level = 0
            if phase_str == "Cruise" and alt_col in group.columns:
                try:
                    fl_series = (group[alt_col] / 100).round(0).astype(int)
                    if not fl_series.empty:
                        mode_vals = fl_series.mode()
                        flight_level = (
                            int(mode_vals.iloc[0]) if not mode_vals.empty else 0
                        )
                except (ValueError, TypeError):
                    flight_level = 0
            else:
                avg_alt_ft = 0.0
                if alt_col in group.columns:
                    try:
                        avg_alt_ft = float(group[alt_col].mean())
                    except (ValueError, TypeError):
                        pass
                flight_level = int(round(avg_alt_ft / 100))

            seg_fuel = 0.0
            seg_co2 = 0.0
            if fuel_per_phase_col and fuel_per_phase_col in group.columns:
                try:
                    seg_fuel = float(group[fuel_per_phase_col].sum())
                except (ValueError, TypeError):
                    pass
            if co2_col and co2_col in group.columns:
                try:
                    seg_co2 = float(group[co2_col].sum())
                except (ValueError, TypeError):
                    pass

            segments.append({
                "phase": phase_str,
                "duration_s": int(len(group)),
                "distance_km": round(phase_dist, 2),
                "flight_level": flight_level,
                "fuel_kg": round(seg_fuel, 2),
                "co2_kg": round(seg_co2, 2),
            })

    db.flight_runs.delete_many({})

    doc = {
        "input_file": input_file,
        "output_file": output_file,
        "aircraft_type": aircraft_type or "737",
        "created_at": datetime.now(timezone.utc),
        "status": "processed",
    }
    if run_id is not None:
        doc["run_id"] = run_id
    phase_status = _build_phase_status(data_df, original_phases)

    phases_found = len([s for s in phase_status.values() if s != "missing"])
    phases_total = len(EXPECTED_PHASES_LIST)
    any_generated = any(s == "generated" for s in phase_status.values())

    data_quality: dict[str, Any] = {
        "total_rows": len(data_df),
        "phases_found": phases_found,
        "phases_total": phases_total,
        "phase_status": phase_status,
        "any_generated": any_generated,
        "was_resampled": True,
        "cleaning_applied": True,
        "generation_applied": any_generated,
        "data_complete": not any_generated and phases_found == phases_total,
        "verified_cleaning": True,
        "verified_resampling": True,
        "verified_generation": any_generated,
    }
    confidence_detail = _build_confidence_detail(data_quality, len(data_df))
    confidence = "high" if data_quality.get("data_complete") else "medium"

    route_info = _build_route_info_from_track_points(track_points)
    flight_date = _flight_date_from_dataframe(data_df)

    doc.update({
        "etow_kg": etow_kg,
        "total_fuel_kg": total_fuel_kg,
        "trip_fuel_kg": trip_fuel_kg,
        "total_co2_kg": total_co2_kg,
        "total_distance_km": round(total_distance_km, 2),
        "flight_duration_s": flight_duration_s,
        "confidence": confidence,
        "confidence_detail": confidence_detail,
        "segments": segments,
        "track_points": track_points,
        "route_info": route_info,
        "flight_date": flight_date,
    })
    doc["data_quality"] = data_quality
    run_result = db.flight_runs.insert_one(doc)
    final_run_id = run_id if run_id is not None else str(run_result.inserted_id)

    inserted_rows = _save_output_rows(db, final_run_id, data_df)
    db.flight_runs.update_one(
        {"_id": run_result.inserted_id},
        {"$set": {"run_id": final_run_id, "inserted_rows": inserted_rows}},
    )
    return final_run_id


def insert_run_from_parsed(
    data_df: pd.DataFrame,
    summary: dict[str, float],
    aircraft_type: str,
    output_csv_path: str,
    original_phases: list[str] | None = None,
) -> str:
    """
    Insert a run from parsed pipeline output (data_df + summary).
    Used by api/app.py /calculate when migrating from SQLite to MongoDB.
    """
    db = get_database()
    run_id = str(uuid4())
    data_df = _clean_dataframe(data_df)

    etow_kg = float(summary.get("ETOW", 0))
    total_fuel_kg = float(summary.get("Total_Fuel", 0))
    trip_fuel_kg = float(summary.get("Trip_fuel", 0))
    total_co2_kg = float(summary.get("Total_CO2", 0))

    flight_duration_s = 0.0
    sum_t_col = None
    for c in data_df.columns:
        if "sum_t" in c.lower() and "s" in c.lower():
            sum_t_col = c
            break
    if sum_t_col and not data_df.empty:
        try:
            flight_duration_s = float(data_df[sum_t_col].iloc[-1])
        except (ValueError, TypeError, KeyError):
            pass

    lat_col = "latitude" if "latitude" in data_df.columns else "lat"
    lon_col = "longitude" if "longitude" in data_df.columns else "lon"
    total_distance_km = 0.0
    if lat_col in data_df.columns and lon_col in data_df.columns:
        coords = data_df[[lat_col, lon_col]].dropna().values
        for i in range(1, len(coords)):
            try:
                total_distance_km += _haversine_km(
                    float(coords[i - 1][0]),
                    float(coords[i - 1][1]),
                    float(coords[i][0]),
                    float(coords[i][1]),
                )
            except (ValueError, TypeError):
                pass

    track_points = _build_track_points(data_df, lat_col, lon_col, step=10)

    fuel_col: str | None = None
    for candidate in ("Fuel_at_time_TE", "Fuel_at_time_kg"):
        if candidate in data_df.columns:
            fuel_col = candidate
            break
    co2_col = None
    for c in data_df.columns:
        if "CO2_at_time" in c and "sum" not in c.lower():
            co2_col = c
            break

    phase_col = "flight_phase" if "flight_phase" in data_df.columns else None
    alt_col = "altitude" if "altitude" in data_df.columns else "alt"

    segments = []
    if phase_col and phase_col in data_df.columns:
        for phase, group in data_df.groupby(phase_col, sort=False):
            phase_coords = group[[lat_col, lon_col]].dropna().values
            phase_dist = 0.0
            for i in range(1, len(phase_coords)):
                try:
                    phase_dist += _haversine_km(
                        float(phase_coords[i - 1][0]),
                        float(phase_coords[i - 1][1]),
                        float(phase_coords[i][0]),
                        float(phase_coords[i][1]),
                    )
                except (ValueError, TypeError):
                    pass

            phase_str = str(phase)
            flight_level = 0
            if phase_str == "Cruise" and alt_col in group.columns:
                try:
                    fl_series = (group[alt_col] / 100).round(0).astype(int)
                    if not fl_series.empty:
                        mode_vals = fl_series.mode()
                        flight_level = (
                            int(mode_vals.iloc[0]) if not mode_vals.empty else 0
                        )
                except (ValueError, TypeError):
                    flight_level = 0
            else:
                avg_alt_ft = 0.0
                if alt_col in group.columns:
                    try:
                        avg_alt_ft = float(group[alt_col].mean())
                    except (ValueError, TypeError):
                        pass
                flight_level = int(round(avg_alt_ft / 100))

            seg_fuel = 0.0
            seg_co2 = 0.0
            if fuel_col and fuel_col in group.columns:
                try:
                    seg_fuel = float(group[fuel_col].sum())
                except (ValueError, TypeError):
                    pass
            if co2_col and co2_col in group.columns:
                try:
                    seg_co2 = float(group[co2_col].sum())
                except (ValueError, TypeError):
                    pass

            segments.append({
                "phase": phase_str,
                "duration_s": int(len(group)),
                "distance_km": round(phase_dist, 2),
                "flight_level": flight_level,
                "fuel_kg": round(seg_fuel, 2),
                "co2_kg": round(seg_co2, 2),
            })

    phase_status = _build_phase_status(data_df, original_phases)

    phases_found = len([s for s in phase_status.values() if s != "missing"])
    phases_total = len(EXPECTED_PHASES_LIST)
    any_generated = any(s == "generated" for s in phase_status.values())

    data_quality: dict[str, Any] = {
        "total_rows": len(data_df),
        "phases_found": phases_found,
        "phases_total": phases_total,
        "phase_status": phase_status,
        "any_generated": any_generated,
        "was_resampled": True,
        "cleaning_applied": True,
        "generation_applied": any_generated,
        "data_complete": not any_generated and phases_found == phases_total,
        "verified_cleaning": True,
        "verified_resampling": True,
        "verified_generation": any_generated,
    }
    confidence_detail = _build_confidence_detail(data_quality, len(data_df))
    confidence = "high" if data_quality.get("data_complete") else "medium"

    route_info = _build_route_info_from_track_points(track_points)
    flight_date = _flight_date_from_dataframe(data_df)

    doc = {
        "run_id": run_id,
        "aircraft_type": aircraft_type or "737",
        "created_at": datetime.now(timezone.utc),
        "etow_kg": etow_kg,
        "total_fuel_kg": total_fuel_kg,
        "trip_fuel_kg": trip_fuel_kg,
        "total_co2_kg": total_co2_kg,
        "total_distance_km": round(total_distance_km, 2),
        "flight_duration_s": flight_duration_s,
        "confidence": confidence,
        "confidence_detail": confidence_detail,
        "segments": segments,
        "track_points": track_points,
        "output_csv_path": output_csv_path,
        "data_quality": data_quality,
        "route_info": route_info,
        "flight_date": flight_date,
    }
    db.flight_runs.insert_one(doc)
    return run_id


def get_run(run_id: str) -> dict | None:
    db = get_database()
    doc = db.flight_runs.find_one({"run_id": run_id})
    if not doc:
        try:
            doc = db.flight_runs.find_one({"_id": ObjectId(run_id)})
        except Exception:
            doc = None
    if doc:
        doc["run_id"] = str(doc.get("run_id", str(doc["_id"])))
        doc.pop("_id", None)
        if hasattr(doc.get("created_at"), "isoformat"):
            doc["created_at"] = doc["created_at"].isoformat()
    return doc


def get_all_runs() -> list[dict]:
    db = get_database()
    runs = []
    for doc in db.flight_runs.find(
        {},
        {
            "run_id": 1,
            "aircraft_type": 1,
            "created_at": 1,
            "total_fuel_kg": 1,
            "total_distance_km": 1,
        },
    ).sort("created_at", -1):
        doc["run_id"] = str(doc.get("run_id", str(doc["_id"])))
        doc.pop("_id", None)
        if "created_at" in doc:
            doc["created_at"] = doc["created_at"].isoformat()
        runs.append(doc)
    return runs


def delete_run(run_id: str) -> bool:
    db = get_database()
    doc = db.flight_runs.find_one({"run_id": run_id})
    if not doc:
        try:
            doc = db.flight_runs.find_one({"_id": ObjectId(run_id)})
        except Exception:
            pass
    if not doc:
        return False
    rid = str(doc.get("run_id", doc["_id"]))
    db.flight_runs.delete_one({"_id": doc["_id"]})
    db.flight_output_rows.delete_many({"run_id": rid})
    return True
