from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database

DEFAULT_MONGO_URI = "mongodb://flight_admin:flight_secret@localhost:27017/flight_app?authSource=admin"
DEFAULT_MONGO_DB = "flight_app"

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



def _extract_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {}

    latest = df.tail(4)
    print(latest)

    keys = ["ETOW", "Total_Fuel", "Trip_fuel", "Total_CO2"]

    return {
        key: latest.loc[latest["utc_time"] == key, "time"].iloc[0] for key in keys
    }

def _save_output_rows(db: Database, run_id: str, output_df: pd.DataFrame, batch_size: int = 1000) -> int:
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


def save_processed_run(*, input_file: str, output_file: str, aircraft_type: str | None) -> str:
    db = get_database()
    output_df = pd.read_csv(output_file)
    summary = _extract_summary(output_df)

    doc = {
        "input_file": input_file,
        "output_file": output_file,
        "aircraft_type": aircraft_type or "737",
        "created_at": datetime.now(timezone.utc),
        "status": "processed",
        "summary": summary,
    }
    run_result = db.flight_runs.insert_one(doc)
    run_id = str(run_result.inserted_id)

    inserted_rows = _save_output_rows(db, run_id, output_df)
    db.flight_runs.update_one(
        {"_id": run_result.inserted_id},
        {"$set": {"inserted_rows": inserted_rows}},
    )

    return run_id

