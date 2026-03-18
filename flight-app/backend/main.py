from contextlib import asynccontextmanager
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from typing import Optional
import tempfile
import os
from pathlib import Path

from process_adsb_pipeline import process
from preprocessing import preprocessing
from db.mongo import (
    connect_to_mongo,
    close_mongo_connection,
    mongo_health_check,
    save_processed_run,
)

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
    aircraft_type: Optional[str] = Form(None)
):
    """
    Accept a CSV file with columns: lat, lon, altitude, timestamp
    and return distance, fuel, mass, and CO2 metrics.
    
    Optional parameter: aircraft_type (e.g., "737", "320")
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    try:
        # Create temp file to store uploaded CSV
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_input_path = tmp_file.name
        
        # Define output path
        output_dir = Path(__file__).parent.parent / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "output.csv"

        # Generate
        preprocessing(tmp_input_path, aircraft_type=aircraft_type or '737')

        # Process the file through the pipeline
        process(
            input_path=tmp_input_path.replace('.csv', '_preprocessed.csv'),
            output_path=str(output_path),
            compute_tas=True,
            aircraft_type=aircraft_type
        )

        run_id = save_processed_run(
            input_file=tmp_input_path,
            output_file=str(output_path),
            aircraft_type=aircraft_type,
        )
        
        # Clean up temp file
        os.unlink(tmp_input_path)
        
        return JSONResponse(content={
            "status": "success",
            "message": "Flight data processed successfully",
            "output_file": str(output_path),
            "run_id": run_id,
        })
        
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to process CSV file: {str(exc)}") from exc


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/health/db")
async def health_check_db():
    try:
        return mongo_health_check()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database connection error: {exc}") from exc


    

