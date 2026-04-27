from contextlib import asynccontextmanager
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from typing import Optional
import tempfile
import os
import uuid
import threading
from pathlib import Path
import asyncio
import json
import time

from process_adsb_pipeline import process
from preprocessing import preprocessing
from open_meteo_cache import ensure_open_meteo_wind_cache_for_csv
from db.mongo import (
    connect_to_mongo,
    close_mongo_connection,
    mongo_health_check,
    save_processed_run,
    get_run,
    get_all_runs,
    delete_run,
    get_original_phases,
    _clean_dataframe,
)

# In-memory progress store for SSE: run_id -> {step, progress, message, done}
progress_store: dict[str, dict] = {}


def update_progress(run_id: str, step: str, progress: int, message: str) -> None:
    done = step == "complete"
    progress_store[run_id] = {
        "step": step,
        "progress": progress,
        "message": message,
        "done": done,
    }
    print(f"[{run_id[:8]}] {progress}% {step}: {message}")


def _run_pipeline_background(
    run_id: str,
    tmp_input_path: str,
    output_path: str,
    aircraft_type: str | None,
) -> None:
    try:
        original_phases = get_original_phases(tmp_input_path)

        preprocessing(tmp_input_path, aircraft_type=aircraft_type or "737")

        update_progress(run_id, "cleaning", 12, "Cleaning data...")
        time.sleep(1.0)

        update_progress(run_id, "resampling", 22, "Resampling data...")
        time.sleep(1.0)

        preprocessed_path = tmp_input_path.replace(".csv", "_preprocessed.csv")
        input_for_pipeline = preprocessed_path
        if not os.path.exists(preprocessed_path):
            # preprocessing.py is defensive and may swallow errors; don't let that
            # crash the pipeline by referencing a missing file.
            input_for_pipeline = tmp_input_path
            update_progress(
                run_id,
                "generating",
                32,
                "Preprocessing output missing; continuing with original CSV...",
            )

        update_progress(run_id, "generating", 30, "Generating missing data...")
        time.sleep(1.2)

        update_progress(run_id, "phases", 38, "Breaking down flight phases...")
        time.sleep(1.0)

        update_progress(run_id, "wind", 46, "Fetching wind data...")
        # Derive lat/lon/time-range from (preprocessed) input and cache wind data.
        # Non-fatal: if it fails, we continue and downstream can still use its own sources (GFS/ERA5).
        try:
            if os.path.exists(input_for_pipeline):
                ensure_open_meteo_wind_cache_for_csv(input_for_pipeline, run_id=run_id)
        except Exception as _exc:
            pass
        time.sleep(1.2)

        process(
            input_path=input_for_pipeline,
            output_path=str(output_path),
            compute_tas=True,
            aircraft_type=aircraft_type,
        )

        update_progress(run_id, "tas", 56, "Calculating True Airspeed...")
        time.sleep(1.0)

        update_progress(run_id, "weight", 65, "Calculating Aircraft Weight...")
        time.sleep(1.0)

        update_progress(run_id, "fuel", 75, "Calculating Fuel Consumption...")
        time.sleep(1.0)

        update_progress(run_id, "co2", 85, "Calculating CO₂ Emissions...")
        time.sleep(1.0)

        update_progress(run_id, "saving", 92, "Saving results data...")
        save_processed_run(
            input_file=tmp_input_path,
            output_file=str(output_path),
            aircraft_type=aircraft_type,
            run_id=run_id,
            original_phases=original_phases,
        )

        if os.path.exists(tmp_input_path):
            os.unlink(tmp_input_path)
        preprocessed_path_obj = Path(preprocessed_path)
        if preprocessed_path_obj.exists():
            try:
                preprocessed_path_obj.unlink()
            except Exception:
                pass

        update_progress(run_id, "complete", 100, "Analysis complete")

    except Exception as exc:
        import traceback

        traceback.print_exc()
        progress_store[run_id] = {
            "step": "error",
            "progress": 0,
            "message": str(exc),
            "done": True,
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_to_mongo()
    yield
    close_mongo_connection()

app = FastAPI(title="Flight App Backend", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Flight App Backend is running"}


@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    aircraft_type: Optional[str] = Form(None),
):
    """
    Accept a CSV file with columns: lat, lon, altitude, timestamp
    and return run_id immediately. Processing runs in background.
    """
    run_id = str(uuid.uuid4())
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_input_path = tmp.name

        # Keep outputs inside the app directory (in-container: /app/output),
        # not at filesystem root (/output).
        output_dir = Path(__file__).resolve().parent / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        # Avoid collisions across concurrent runs
        output_path = output_dir / f"output_{run_id}.csv"

        update_progress(run_id, "uploading", 5, "File received and validated")

        thread = threading.Thread(
            target=_run_pipeline_background,
            args=(run_id, tmp_input_path, str(output_path), aircraft_type),
            daemon=True,
        )
        thread.start()

        return JSONResponse(
            content={
                "status": "processing",
                "message": "Flight data is being processed",
                "run_id": run_id,
            }
        )

    except Exception as exc:
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start processing: {str(exc)}",
        ) from exc


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/health/db")
async def health_check_db():
    try:
        return mongo_health_check()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database connection error: {exc}") from exc


# --- GET /runs ---
@app.get("/runs")
async def list_runs():
    return get_all_runs()


# --- GET /runs/{run_id} ---
@app.get("/runs/{run_id}")
async def get_run_result(run_id: str):
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


# --- DELETE /runs/{run_id} ---
@app.delete("/runs/{run_id}")
async def remove_run(run_id: str):
    deleted = delete_run(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"deleted": True}


# --- GET /runs/{run_id}/chart-data ---
@app.get("/runs/{run_id}/chart-data")
async def get_chart_data(run_id: str):
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    output_file = run.get("output_file") or run.get("output_csv_path")
    if not output_file or not os.path.exists(output_file):
        raise HTTPException(status_code=404, detail="Output file not found")
    import pandas as pd
    import numpy as np
    SUMMARY_LABELS = ["ETOW", "Total_Fuel", "Trip_fuel", "Total_CO2"]
    df = pd.read_csv(output_file)
    df = _clean_dataframe(df)
    first_col = df.columns[0]
    df = df[~df[first_col].astype(str).str.strip().isin(SUMMARY_LABELS)].copy().reset_index(drop=True)

    def _pick_col(*candidates: str) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    sum_t_col = _pick_col("sum_t (s)", "sum_t(s)", "sum_t")
    time_vals = df[sum_t_col].fillna(0) if sum_t_col else pd.Series([0] * len(df))

    def fmt_time(s: float) -> str:
        sec = int(s)
        return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}"

    time_labels = [fmt_time(float(v)) for v in time_vals]

    def safe(col: str | None) -> list[float]:
        if not col or col not in df.columns:
            return [0.0] * len(df)
        return (
            df[col]
            .fillna(0)
            .replace([np.inf, -np.inf], 0)
            .round(4)
            .tolist()
        )

    mass_col = _pick_col("mt", "mass_kg")
    alt_col = _pick_col("altitude", "alt")
    tas_col = _pick_col("TAS_kt", "TAS")
    gs_col = _pick_col("ground_speed", "Speed", "gs", "GS")
    fuel_rate_col = _pick_col("Fuel_kg_per_s")
    fuel_sum_col = _pick_col("Fuel_sum_with_time_TE", "Fuel_sum_with_time_kg")
    co2_at_col = _pick_col("CO2_at_time", "CO2_at_time_TE")
    co2_sum_col = _pick_col("CO2_sum_with_time")

    altitude_fl = (
        (df[alt_col] / 100).round(1).tolist()
        if alt_col
        else [0.0] * len(df)
    )
    # True airspeed and ground speed in knots (pipeline stores both in kt).
    tas_kt = (
        df[tas_col]
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .round(4)
        .tolist()
        if tas_col
        else [0.0] * len(df)
    )
    ground_speed_kt = (
        df[gs_col]
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
        .round(4)
        .tolist()
        if gs_col
        else [0.0] * len(df)
    )
    weight_kg = safe(mass_col) if mass_col else [0.0] * len(df)
    fuel_flow_kgh = (
        (df[fuel_rate_col] * 3600).round(4).tolist()
        if fuel_rate_col
        else [0.0] * len(df)
    )
    total_fuel_kg = safe(fuel_sum_col)
    co2_flow_kgh = (
        (df[co2_at_col] * 3600).round(4).tolist()
        if co2_at_col
        else [0.0] * len(df)
    )
    total_co2_kg = safe(co2_sum_col)

    return {
        "time_labels": time_labels,
        "altitude_fl": altitude_fl,
        "tas_kt": tas_kt,
        "ground_speed_kt": ground_speed_kt,
        "weight_kg": weight_kg,
        "fuel_flow_kgh": fuel_flow_kgh,
        "total_fuel_kg": total_fuel_kg,
        "co2_flow_kgh": co2_flow_kgh,
        "total_co2_kg": total_co2_kg,
    }


# --- GET /runs/{run_id}/export ---
@app.get("/runs/{run_id}/export")
async def export_run(run_id: str):
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    output_file = run.get("output_file") or run.get("output_csv_path")
    if not output_file or not os.path.exists(output_file):
        raise HTTPException(status_code=404, detail="Output file not found")
    return FileResponse(
        path=output_file,
        media_type="text/csv",
        filename=f"flight_{run_id[:8]}.csv",
        headers={"Content-Disposition": f"attachment; filename=flight_{run_id[:8]}.csv"},
    )


# --- GET /runs/{run_id}/progress (SSE) ---
@app.get("/runs/{run_id}/progress")
async def run_progress(run_id: str):
    async def event_stream():
        start = time.monotonic()
        # 0 = wait indefinitely (no app-level timeout)
        timeout_s = int(os.getenv("RUN_PROGRESS_TIMEOUT_S", "0"))
        heartbeat_s = float(os.getenv("RUN_PROGRESS_HEARTBEAT_S", "15"))
        poll_interval = 0.5
        last_state = None
        last_heartbeat = time.monotonic()
        try:
            while True:
                elapsed = time.monotonic() - start
                # Allow disabling app-level SSE timeout by setting RUN_PROGRESS_TIMEOUT_S=0
                if timeout_s > 0 and elapsed > timeout_s:
                    break
                state = progress_store.get(run_id)
                # Heartbeat to keep SSE connection alive even when state doesn't change.
                now = time.monotonic()
                if heartbeat_s > 0 and (now - last_heartbeat) >= heartbeat_s:
                    yield ": ping\n\n"
                    last_heartbeat = now
                if state is not None:
                    if state.get("step") == "error":
                        error_data = json.dumps({
                            "step": "error",
                            "progress": 0,
                            "message": state.get("message", "Processing failed"),
                        })
                        yield f"data: {error_data}\n\n"
                        break
                    data = json.dumps({
                        "step": state["step"],
                        "progress": state["progress"],
                        "message": state["message"],
                    })
                    if state != last_state:
                        yield f"data: {data}\n\n"
                        last_state = dict(state)
                        last_heartbeat = time.monotonic()
                    if state.get("done"):
                        break
                else:
                    pending = {"step": "pending", "progress": 0, "message": "Waiting for processing to start..."}
                    if last_state is None:
                        yield f"data: {json.dumps(pending)}\n\n"
                        last_state = pending
                        last_heartbeat = time.monotonic()
                await asyncio.sleep(poll_interval)
        finally:
            progress_store.pop(run_id, None)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )

