import axios from 'axios';
// @ts-ignore - JSON import
import mockResult from '../mocks/sampleResult.json';

export type AircraftType = 'airbus' | 'boeing';

export interface FlightTrackPoint {
  lat: number;
  lon: number;
  timestamp: string;
  altitude: number;
  speed: number;
}

export interface FlightSegment {
  phase: string;
  duration: string;
  distance_km: number;
  flight_level: string;
  fuel_kg: number;
}

export interface FlightSummary {
  distance_nm: number;
  distance_km: number;
  time_enroute: string;
  fuel_kg: number;
  co2_kg: number;
  mass_kg: number;
}

export interface FlightAnalysisResult {
  summary: FlightSummary;
  track: FlightTrackPoint[];
  segments: FlightSegment[];
  flight_fuel_kg: number;
  block_fuel_kg: number;
}

/**
 * API Adapter for Flight Analysis
 * Currently uses mock data, but can be switched to real backend by changing the URL
 */
export async function submitFlightCsv(
  file: File,
  aircraftType: AircraftType
): Promise<FlightAnalysisResult> {
  // TODO: Replace with real backend when ready
  // const formData = new FormData();
  // formData.append('file', file);
  // formData.append('aircraftType', aircraftType);
  // const response = await axios.post<FlightAnalysisResult>(
  //   process.env.NEXT_PUBLIC_BACKEND_URL + '/upload',
  //   formData
  // );
  // return response.data;

  // Simulate API delay
  await new Promise((resolve) => setTimeout(resolve, 1500));

  // Return mock data
  return mockResult as FlightAnalysisResult;
}

