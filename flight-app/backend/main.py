from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from calc import compute_flight_metrics_from_csv

app = FastAPI(title="Flight App Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """
    Accept a CSV file with columns: lat, lon, altitude, timestamp
    and return distance, fuel, mass, and CO2 metrics.
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    try:
        # UploadFile.file is a SpooledTemporaryFile (file-like)
        metrics = compute_flight_metrics_from_csv(file.file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - generic safety net
        raise HTTPException(status_code=500, detail="Failed to process CSV file.") from exc

    return JSONResponse(content=metrics)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


