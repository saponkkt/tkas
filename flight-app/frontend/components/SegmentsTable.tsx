'use client';

import type { Segment } from '@/lib/api';
import { formatDuration, formatNumber } from '@/lib/formatters';

const PHASE_DISPLAY: Record<string, string> = {
  Taxi_out: 'Taxi-out',
  Takeoff: 'Take-off',
  Initial_climb: 'Initial climb',
  Climb: 'Climb',
  Cruise: 'Cruise',
  Descent: 'Descent',
  Approach: 'Approach',
  Landing: 'Landing',
  Taxi_in: 'Taxi-in',
};

const PHASE_ORDER = [
  'Taxi_out',
  'Takeoff',
  'Initial_climb',
  'Climb',
  'Cruise',
  'Descent',
  'Approach',
  'Landing',
  'Taxi_in',
];

const TRIP_PHASES = [
  'Takeoff',
  'Initial_climb',
  'Climb',
  'Cruise',
  'Descent',
  'Approach',
  'Landing',
];

const PHASE_STYLES: Record<string, string> = {
  Taxi_out: 'bg-gray-100 text-gray-600',
  Taxi_in: 'bg-gray-100 text-gray-600',
  Takeoff: 'bg-orange-100 text-orange-600',
  Initial_climb: 'bg-orange-100 text-orange-600',
  Climb: 'bg-blue-100 text-blue-600',
  Cruise: 'bg-green-100 text-green-600',
  Descent: 'bg-yellow-100 text-yellow-600',
  Approach: 'bg-yellow-100 text-yellow-600',
  Landing: 'bg-red-100 text-red-600',
};

function phaseClass(phase: string): string {
  return PHASE_STYLES[phase] ?? 'bg-gray-100 text-gray-600';
}

interface SegmentsTableProps {
  segments: Segment[];
  tripFuelKg: number;
  totalFuelKg: number;
  totalDistanceKm: number;
  flightDurationS: number;
  totalCo2Kg: number;
}

export default function SegmentsTable({
  segments,
  tripFuelKg,
  totalFuelKg,
  totalDistanceKm,
  flightDurationS,
  totalCo2Kg,
}: SegmentsTableProps) {
  const rawTotalDuration = segments.reduce((s, x) => s + x.duration_s, 0);
  const rawTotalDistance = segments.reduce((s, x) => s + x.distance_km, 0);
  const rawTotalFuel = segments.reduce((s, x) => s + x.fuel_kg, 0);
  const rawTotalCO2 = segments.reduce((s, x) => s + x.co2_kg, 0);

  const scaleDuration =
    rawTotalDuration > 0 ? flightDurationS / rawTotalDuration : 1;
  const scaleDistance =
    rawTotalDistance > 0 ? totalDistanceKm / rawTotalDistance : 1;
  const scaleFuel = rawTotalFuel > 0 ? totalFuelKg / rawTotalFuel : 1;
  const scaleCO2 = rawTotalCO2 > 0 ? totalCo2Kg / rawTotalCO2 : 1;

  const scaledSegments = segments.map((seg) => ({
    ...seg,
    duration_s: Math.round(seg.duration_s * scaleDuration),
    distance_km: +(seg.distance_km * scaleDistance).toFixed(2),
    fuel_kg: +(seg.fuel_kg * scaleFuel).toFixed(2),
    co2_kg: +(seg.co2_kg * scaleCO2).toFixed(2),
  }));

  const sortedSegments = [...scaledSegments].sort((a, b) => {
    const ai = PHASE_ORDER.indexOf(a.phase);
    const bi = PHASE_ORDER.indexOf(b.phase);
    return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
  });

  const cruiseSeg = segments.find((s) => s.phase === 'Cruise');
  const cruiseFL = cruiseSeg?.flight_level ?? 0;

  const tripSegs = scaledSegments.filter((s) => TRIP_PHASES.includes(s.phase));
  const tripDuration = tripSegs.reduce((s, x) => s + x.duration_s, 0);
  const tripDistance = tripSegs.reduce((s, x) => s + x.distance_km, 0);
  const tripCO2 = tripSegs.reduce((s, x) => s + x.co2_kg, 0);

  const totalDuration = flightDurationS;
  const totalDistance = totalDistanceKm;
  const totalFuel = totalFuelKg;
  const totalCO2 = scaledSegments.reduce((s, x) => s + x.co2_kg, 0);
  const tripFL = cruiseFL;
  const totalFL = cruiseFL;

  return (
    <div className="bg-white rounded-xl shadow-sm overflow-hidden flex flex-col h-full">
      <div className="overflow-x-auto flex-1">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr className="border-b border-gray-200">
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4 w-32">
                PHASE
              </th>
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4">
                DURATION
              </th>
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4">
                DISTANCE (KM)
              </th>
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4">
                FL
              </th>
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4">
                FUEL (KG)
              </th>
              <th className="text-center text-xs font-semibold text-gray-400 tracking-wider uppercase py-4 px-4">
                CO₂ (KG)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedSegments.map((seg, idx) => (
              <tr key={idx} className="hover:bg-gray-50">
                <td className="text-left py-4 px-4">
                  <span
                    className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${phaseClass(seg.phase)}`}
                  >
                    {PHASE_DISPLAY[seg.phase] ?? seg.phase}
                  </span>
                </td>
                <td className="text-center py-4 px-4 font-mono text-sm text-gray-700 whitespace-nowrap">
                  {formatDuration(seg.duration_s)}
                </td>
                <td className="text-center py-4 px-4 text-sm text-gray-700 whitespace-nowrap">
                  {formatNumber(Math.round(seg.distance_km))}
                </td>
                <td className="text-center py-4 px-4 text-sm text-gray-700 whitespace-nowrap">
                  {seg.flight_level}
                </td>
                <td className="text-center py-4 px-4 text-sm text-gray-700 whitespace-nowrap">
                  {formatNumber(Math.round(seg.fuel_kg))}
                </td>
                <td className="text-center py-4 px-4 text-sm text-gray-700 whitespace-nowrap">
                  {formatNumber(Math.round(seg.co2_kg))}
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot className="bg-slate-50 border-t-2 border-slate-200">
            <tr className="bg-slate-50">
              <td className="text-left py-3 px-4 text-xs font-semibold text-gray-500 tracking-wider uppercase">
                TRIP FLIGHT
              </td>
              <td className="text-center py-3 px-4 font-mono text-sm font-semibold text-gray-800">
                {formatDuration(tripDuration)}
              </td>
              <td className="text-center py-3 px-4 text-sm font-semibold text-gray-800">
                {Math.round(tripDistance).toLocaleString()}
              </td>
              <td className="text-center py-3 px-4 text-sm font-semibold text-gray-800">
                {tripFL}
              </td>
              <td className="text-center py-3 px-4 text-sm font-bold text-blue-600">
                {Math.round(tripFuelKg).toLocaleString()}
              </td>
              <td className="text-center py-3 px-4 text-sm font-bold text-blue-600">
                {Math.round(tripCO2).toLocaleString()}
              </td>
            </tr>
            <tr className="bg-slate-50">
              <td className="text-left py-3 px-4 text-xs font-semibold text-gray-500 tracking-wider uppercase">
                TOTAL FLIGHT
              </td>
              <td className="text-center py-3 px-4 font-mono text-sm font-semibold text-gray-800">
                {formatDuration(totalDuration)}
              </td>
              <td className="text-center py-3 px-4 text-sm font-semibold text-gray-800">
                {Math.round(totalDistance).toLocaleString()}
              </td>
              <td className="text-center py-3 px-4 text-sm font-semibold text-gray-800">
                {totalFL}
              </td>
              <td className="text-center py-3 px-4 text-sm font-bold text-gray-900">
                {Math.round(totalFuel).toLocaleString()}
              </td>
              <td className="text-center py-3 px-4 text-sm font-bold text-gray-900">
                {Math.round(totalCO2).toLocaleString()}
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
