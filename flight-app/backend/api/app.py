"""
FastAPI wrapper for the existing ADS-B processing pipeline.
- POST /calculate: upload CSV + aircraft_type, run pipeline, store in MongoDB, return run_id.
- GET /summary/{run_id}, /track/{run_id}, /segments/{run_id}, /download/csv/{run_id}.
- GET /runs, GET /runs/{run_id}, DELETE /runs/{run_id}, GET /runs/{run_id}/export, GET /runs/{run_id}/progress.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

# Backend root (parent of api/)
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from db.mongo import (
    connect_to_mongo,
    close_mongo_connection,
    get_run,
    get_all_runs,
    delete_run,
    insert_run_from_parsed,
    get_original_phases as _get_original_phases,
)
from api.parse_output_csv import parse_output_csv

# --- SQLite (kept as comments for rollback) ---
# from db.helpers import (
#     ensure_schema,
#     get_run,
#     get_track,
#     get_segments,
#     insert_run,
#     insert_track_rows,
#     insert_segment_rows,
# )
# from db.schema import DB_PATH
# from api.parse_output_csv import (
#     parse_output_csv,
#     data_to_track_rows,
#     data_to_segment_rows,
# )


@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_to_mongo()
    yield
    close_mongo_connection()


app = FastAPI(title="Flight ADS-B Pipeline API", version="1.0.0", lifespan=lifespan)

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


# --- SQLite startup (kept for rollback) ---
# @app.on_event("startup")
# def startup():
#     ensure_schema()


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

    original_phases = _get_original_phases(input_path)

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

    # Store in MongoDB
    run_id = insert_run_from_parsed(
        data_df=data_df,
        summary=summary,
        aircraft_type=aircraft_type,
        output_csv_path=output_path,
        original_phases=original_phases,
    )

    # --- SQLite (kept for rollback) ---
    # etow = summary.get("ETOW")
    # total_fuel = summary.get("Total_Fuel")
    # trip_fuel = summary.get("Trip_fuel")
    # total_co2 = summary.get("Total_CO2")
    # run_id = insert_run(
    #     aircraft_type=aircraft_type,
    #     output_csv_path=output_path,
    #     etow_kg=etow,
    #     total_fuel_kg=total_fuel,
    #     trip_fuel_kg=trip_fuel,
    #     total_co2_kg=total_co2,
    # )
    # track_rows = data_to_track_rows(data_df)
    # segment_rows = data_to_segment_rows(data_df)
    # insert_track_rows(run_id, track_rows)
    # insert_segment_rows(run_id, segment_rows)

    return {"run_id": run_id}


@app.get("/summary/{run_id}")
async def summary(run_id: str):
    """Return flight_run summary for the given run_id."""
    doc = get_run(run_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "run_id": str(doc.get("run_id", "")),
        "aircraft_type": doc.get("aircraft_type", ""),
        "created_at": doc["created_at"].isoformat()
        if hasattr(doc.get("created_at"), "isoformat")
        else str(doc.get("created_at", "")),
        "etow_kg": doc.get("etow_kg", 0),
        "total_fuel_kg": doc.get("total_fuel_kg", 0),
        "trip_fuel_kg": doc.get("trip_fuel_kg", 0),
        "total_co2_kg": doc.get("total_co2_kg", 0),
        "total_distance_km": doc.get("total_distance_km", 0),
        "flight_duration_s": doc.get("flight_duration_s", 0),
        "confidence": doc.get("confidence", "high"),
        "confidence_detail": doc.get("confidence_detail", ""),
    }


@app.get("/track/{run_id}")
async def track(run_id: str):
    """Return track_points for map rendering (lat, lon)."""
    doc = get_run(run_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"track_points": doc.get("track_points", [])}


@app.get("/segments/{run_id}")
async def segments(run_id: str):
    """Return flight segments (phase, duration_s, distance_km, flight_level, fuel_kg, co2_kg)."""
    doc = get_run(run_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"segments": doc.get("segments", [])}


@app.get("/download/csv/{run_id}")
async def download_csv(run_id: str):
    """Stream the original pipeline output CSV. Content-Disposition set for download."""
    doc = get_run(run_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Run not found")
    path = doc.get("output_csv_path") or doc.get("output_file")
    if not path or not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Output CSV file no longer available")
    filename = f"flight_output_{run_id}.csv"
    return FileResponse(
        path,
        media_type="text/csv",
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --- New endpoints (MongoDB) ---

@app.get("/runs")
async def list_runs():
    """List all runs (sorted by created_at descending)."""
    return get_all_runs()


@app.get("/runs/{run_id}")
async def get_run_full(run_id: str):
    """Return full run data: summary + track_points + segments (for result page)."""
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.delete("/runs/{run_id}")
async def remove_run(run_id: str):
    """Delete a run by id."""
    deleted = delete_run(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"deleted": True}


@app.get("/runs/{run_id}/export")
async def export_run(run_id: str):
    """Download output CSV for the run."""
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    path = run.get("output_csv_path") or run.get("output_file")
    if not path or not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Output file not found")
    filename = f"flight_{run_id[:8]}.csv"
    return FileResponse(
        path,
        media_type="text/csv",
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/runs/{run_id}/progress")
async def run_progress(run_id: str):
    """SSE stream for processing progress (simulated steps)."""
    async def event_stream():
        steps = [
            ("uploading", 15, "File received and validated"),
            ("preprocessing", 30, "Resampling ADS-B data..."),
            ("preprocessing", 50, "Generating takeoff and landing phases..."),
            ("calculating", 65, "Computing fuel consumption..."),
            ("calculating", 80, "Computing CO₂ emissions and energy..."),
            ("saving", 92, "Saving results to database..."),
            ("complete", 100, "Analysis complete"),
        ]
        for step, progress, message in steps:
            data = json.dumps({
                "step": step,
                "progress": progress,
                "message": message,
            })
            yield f"data: {data}\n\n"
            await asyncio.sleep(1.8)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
