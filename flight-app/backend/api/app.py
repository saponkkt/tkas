"""
FastAPI wrapper for the existing ADS-B processing pipeline.
- POST /calculate: upload CSV + aircraft_type, run pipeline, store in SQLite, return run_id.
- GET /summary/{run_id}, /track/{run_id}, /segments/{run_id}, /download/csv/{run_id}.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# Backend root (parent of api/)
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from db.helpers import (
    ensure_schema,
    get_run,
    get_track,
    get_segments,
    insert_run,
    insert_track_rows,
    insert_segment_rows,
)
from db.schema import DB_PATH
from api.parse_output_csv import (
    parse_output_csv,
    data_to_track_rows,
    data_to_segment_rows,
)

app = FastAPI(title="Flight ADS-B Pipeline API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _run_pipeline(input_path: str, output_path: str, aircraft_type: str) -> None:
    """Execute process_adsb_pipeline.py. Raises on non-zero exit."""
    cmd = [
        sys.executable,
        str(BACKEND_DIR / "process_adsb_pipeline.py"),
        input_path,
        output_path,
        aircraft_type,
    ]
    result = subprocess.run(
        cmd,
        cwd=str(BACKEND_DIR),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline failed (exit {result.returncode}): {result.stderr or result.stdout}"
        )


@app.on_event("startup")
def startup():
    ensure_schema()


@app.post("/calculate")
async def calculate(
    file: UploadFile = File(...),
    aircraft_type: str = Form(...),
):
    """
    Upload ADS-B CSV and run the pipeline. Returns run_id.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    aircraft_type = (aircraft_type or "").strip()
    if not aircraft_type:
        raise HTTPException(status_code=400, detail="aircraft_type is required.")

    # Use a temporary directory so we can delete input after run and keep output for download
    tmpdir = tempfile.mkdtemp(prefix="flight_")
    input_path = os.path.join(tmpdir, "input.csv")
    output_path = os.path.join(tmpdir, "output.csv")

    try:
        contents = await file.read()
        with open(input_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}") from e

    try:
        _run_pipeline(input_path, output_path, aircraft_type)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        # Delete input CSV immediately after processing
        try:
            os.unlink(input_path)
        except OSError:
            pass

    if not os.path.isfile(output_path):
        raise HTTPException(status_code=500, detail="Pipeline did not produce output CSV.")

    # Parse output CSV: time-series + summary
    data_df, summary = parse_output_csv(output_path)

    etow = summary.get("ETOW")
    total_fuel = summary.get("Total_Fuel")
    trip_fuel = summary.get("Trip_fuel")
    total_co2 = summary.get("Total_CO2")

    # Store in SQLite (output_csv_path = path to keep for download)
    run_id = insert_run(
        aircraft_type=aircraft_type,
        output_csv_path=output_path,
        etow_kg=etow,
        total_fuel_kg=total_fuel,
        trip_fuel_kg=trip_fuel,
        total_co2_kg=total_co2,
    )

    track_rows = data_to_track_rows(data_df)
    segment_rows = data_to_segment_rows(data_df)
    insert_track_rows(run_id, track_rows)
    insert_segment_rows(run_id, segment_rows)

    return {"run_id": run_id}


@app.get("/summary/{run_id}")
async def summary(run_id: str):
    """Return flight_run summary for the given run_id."""
    row = get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {
        "run_id": row["run_id"],
        "aircraft_type": row["aircraft_type"],
        "created_at": row["created_at"],
        "etow_kg": row["etow_kg"],
        "total_fuel_kg": row["total_fuel_kg"],
        "trip_fuel_kg": row["trip_fuel_kg"],
        "total_co2_kg": row["total_co2_kg"],
    }


@app.get("/track/{run_id}")
async def track(run_id: str):
    """Return flight_track rows for map rendering (lat, lon, timestamp, altitude, speed, flight_phase)."""
    row = get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    rows = get_track(run_id)
    return {"run_id": run_id, "points": rows}


@app.get("/segments/{run_id}")
async def segments(run_id: str):
    """Return flight_segment rows (timestamp, delta_t_s, fuel_kg, co2_kg)."""
    row = get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    rows = get_segments(run_id)
    return {"run_id": run_id, "segments": rows}


@app.get("/download/csv/{run_id}")
async def download_csv(run_id: str):
    """Stream the original pipeline output CSV. Content-Disposition set for download."""
    row = get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    path = row["output_csv_path"]
    if not path or not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Output CSV file no longer available.")
    filename = f"flight_output_{run_id}.csv"
    return FileResponse(
        path,
        media_type="text/csv",
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
