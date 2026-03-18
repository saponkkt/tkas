/**
 * API client for Flight ADS-B Pipeline backend.
 * All data comes from SQL-backed APIs; CSV download streams the pipeline output.
 */

const BASE_URL = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';

export type AircraftType = 'airbus' | 'boeing';

export interface FlightTrackPoint {
  lat: number;
  lon: number;
  timestamp: string;
  altitude?: number;
  speed?: number;
  flight_phase?: string;
}

export interface FlightSegmentRow {
  timestamp: string;
  delta_t_s: number | null;
  fuel_kg: number | null;
  co2_kg: number | null;
}

export interface FlightSummary {
  run_id: string;
  aircraft_type: string;
  created_at: string;
  etow_kg: number | null;
  total_fuel_kg: number | null;
  trip_fuel_kg: number | null;
  total_co2_kg: number | null;
}

export interface FlightAnalysisResult {
  run_id: string;
  summary: FlightSummary;
  track: FlightTrackPoint[];
  segments: FlightSegmentRow[];
  total_fuel_kg: number | null;
  trip_fuel_kg: number | null;
}

/** POST /upload: upload CSV + aircraft_type, returns processed data directly */
export async function uploadFlightCsv(
  file: File,
  aircraftType: AircraftType | string
): Promise<{ 
  summary: {
    aircraft_type: string;
    etow_kg: number | null;
    total_fuel_kg: number | null;
    trip_fuel_kg: number | null;
    total_co2_kg: number | null;
  };
  track: Array<{
    latitude: number;
    longitude: number;
    timestamp: string;
    altitude?: number;
    speed?: number;
    flight_phase?: string;
  }>;
  output_file: string;
}> {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('aircraft_type', aircraftType === 'airbus' ? 'A320' : (aircraftType === 'boeing' ? '737' : aircraftType));
  
  const response = await fetch(`${BASE_URL}/upload`, {
    method: 'POST',
    body: formData,
  });
  
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Upload failed: ${response.status}`);
  }
  
  return response.json();
}

/** POST /calculate: upload CSV + aircraft_type, returns run_id */
export async function submitFlightCsv(
  file: File,
  aircraftType: AircraftType
): Promise<{ run_id: string }> {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('aircraft_type', aircraftType === 'airbus' ? 'Airbus' : 'Boeing');
  const response = await fetch(`${BASE_URL}/calculate`, {
    method: 'POST',
    body: formData,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Upload failed: ${response.status}`);
  }
  return response.json();
}

/** GET /summary/{run_id} */
export async function fetchSummary(runId: string): Promise<FlightSummary> {
  const response = await fetch(`${BASE_URL}/summary/${runId}`);
  if (!response.ok) {
    if (response.status === 404) throw new Error('Run not found');
    throw new Error(`Failed to fetch summary: ${response.status}`);
  }
  return response.json();
}

/** GET /track/{run_id} → { run_id, points } */
export async function fetchTrack(runId: string): Promise<{ run_id: string; points: Array<{ timestamp: string; latitude: number; longitude: number; altitude?: number; speed?: number; flight_phase?: string }> }> {
  const response = await fetch(`${BASE_URL}/track/${runId}`);
  if (!response.ok) {
    if (response.status === 404) throw new Error('Run not found');
    throw new Error(`Failed to fetch track: ${response.status}`);
  }
  return response.json();
}

/** GET /segments/{run_id} → { run_id, segments } */
export async function fetchSegments(runId: string): Promise<{ run_id: string; segments: FlightSegmentRow[] }> {
  const response = await fetch(`${BASE_URL}/segments/${runId}`);
  if (!response.ok) {
    if (response.status === 404) throw new Error('Run not found');
    throw new Error(`Failed to fetch segments: ${response.status}`);
  }
  return response.json();
}

/** GET /download/csv/{run_id} — triggers browser download of pipeline output CSV */
export function downloadProcessedCsv(runId: string): void {
  const url = `${BASE_URL}/download/csv/${runId}`;
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `flight_output_${runId}.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

/** Normalize track API points to FlightTrackPoint[] for map/UI */
export function trackPointsToMapFormat(points: Array<{ timestamp: string; latitude: number; longitude: number; altitude?: number; speed?: number; flight_phase?: string }>): FlightTrackPoint[] {
  return points.map((p) => ({
    lat: p.latitude,
    lon: p.longitude,
    timestamp: p.timestamp,
    altitude: p.altitude ?? undefined,
    speed: p.speed ?? undefined,
    flight_phase: p.flight_phase ?? undefined,
  }));
}
