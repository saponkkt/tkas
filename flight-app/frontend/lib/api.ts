const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface Segment {
  phase: string;
  duration_s: number;
  distance_km: number;
  flight_level: number;
  fuel_kg: number;
  co2_kg: number;
}

export interface AirportInfo {
  iata: string;
  icao: string;
  name: string;
  city: string;
  country?: string;
  country_code: string;
  lat?: number;
  lon?: number;
}

export interface RouteInfo {
  departure: AirportInfo;
  arrival: AirportInfo;
}

export interface DataQuality {
  total_rows: number;
  phases_found: number;
  phases_total: number;
  phase_status: Record<string, 'original' | 'generated' | 'missing'>;
  any_generated: boolean;
  was_resampled: boolean;
  cleaning_applied: boolean;
  generation_applied: boolean;
  data_complete: boolean;
  verified_cleaning: boolean;
  verified_resampling: boolean;
  verified_generation: boolean;
}

export interface RunResult {
  run_id: string;
  aircraft_type: string;
  created_at: string;
  flight_date?: string;
  etow_kg: number;
  total_fuel_kg: number;
  trip_fuel_kg: number;
  total_co2_kg: number;
  total_distance_km: number;
  flight_duration_s: number;
  confidence: string;
  confidence_detail: string;
  segments: Segment[];
  track_points: Array<{ lat: number; lon: number; ground_speed?: number }>;
  data_quality?: DataQuality;
  route_info?: RouteInfo | null;
}

export interface ChartData {
  time_labels: string[];
  altitude_fl: number[];
  /** True airspeed, knots */
  tas_kt: number[];
  /** Ground speed (ADS-B or derived), knots */
  ground_speed_kt?: number[];
  weight_kg: number[];
  fuel_flow_kgh: number[];
  total_fuel_kg: number[];
  co2_flow_kgh: number[];
  total_co2_kg: number[];
}

export async function getChartData(runId: string): Promise<ChartData> {
  const res = await fetch(`${API_URL}/runs/${runId}/chart-data`);
  if (!res.ok) throw new Error('Chart data not found');
  return res.json();
}

export async function uploadFlight(
  file: File,
  aircraftType: string
): Promise<{ run_id: string }> {
  const form = new FormData();
  form.append('file', file);
  form.append('aircraft_type', aircraftType);
  const res = await fetch(`${API_URL}/upload`, { method: 'POST', body: form });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Upload failed' }));
    throw new Error(
      typeof err.detail === 'string' ? err.detail : JSON.stringify(err.detail || 'Upload failed')
    );
  }
  return res.json();
}

export async function getRun(runId: string): Promise<RunResult> {
  const res = await fetch(`${API_URL}/runs/${runId}`);
  if (!res.ok) throw new Error('Run not found');
  return res.json();
}

export async function exportRun(runId: string): Promise<void> {
  const res = await fetch(`${API_URL}/runs/${runId}/export`);
  if (!res.ok) throw new Error('Export failed');
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `flight_${runId.slice(0, 8)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}
