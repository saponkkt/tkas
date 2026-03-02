def compute_flight_metrics_from_csv(
    file_obj: Union[BinaryIO, IOBase]
) -> Dict[str, float]:
    """
    อ่าน FlightRadar24-like CSV และคำนวณ distance, fuel, mass, CO2
    ใช้สำหรับ backend FastAPI /upload
    """
    df = pd.read_csv(file_obj)
    df = _ensure_required_columns(df)

    if len(df) < 2:
        # Not enough points to form a segment
        return {
            "distance_nm": 0.0,
            "fuel_kg": 0.0,
            "mass_kg": 0.0,
            "co2_kg": 0.0,
        }