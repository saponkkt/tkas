import React from 'react';
import { FlightSegmentRow } from '@/services/flightApi';

interface SegmentTableProps {
  segments: FlightSegmentRow[];
  totalFuelKg: number | null;
}

function fmt(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return '—';
  return v.toLocaleString('en-US', { maximumFractionDigits: 3 });
}

export default function SegmentTable({
  segments,
  totalFuelKg,
}: SegmentTableProps) {
  return (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden flex flex-col h-full">
      <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
        <h2 className="text-xl font-semibold text-gray-900">
          Flight Segments
        </h2>
        <p className="text-sm text-gray-600 mt-1">
          Time-slice data: Δt (s), fuel (kg), CO₂ (kg)
        </p>
      </div>

      <div className="overflow-x-auto flex-1">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Timestamp (UTC)
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Δt (s)
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Fuel (kg)
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                CO₂ (kg)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {segments.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-8 text-center text-gray-500">
                  No segment data
                </td>
              </tr>
            ) : (
              segments.map((seg, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {seg.timestamp ? new Date(seg.timestamp).toISOString().replace('T', ' ').slice(0, 19) : '—'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                    {fmt(seg.delta_t_s)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                    {fmt(seg.fuel_kg)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                    {fmt(seg.co2_kg)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
          <tfoot className="bg-gray-50 border-t-2 border-gray-300">
            <tr>
              <td colSpan={2} className="px-6 py-3 text-sm font-semibold text-gray-900 text-right">
                Total Fuel (kg):
              </td>
              <td className="px-6 py-3 text-sm font-bold text-gray-900 text-right" colSpan={2}>
                {totalFuelKg != null ? totalFuelKg.toLocaleString('en-US', { maximumFractionDigits: 2 }) : '—'}
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
