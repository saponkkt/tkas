import React from 'react';
import { FlightSegment, FlightAnalysisResult } from '@/services/flightApi';

interface SegmentTableProps {
  segments: FlightSegment[];
  flightFuel: number;
  blockFuel: number;
}

export default function SegmentTable({
  segments,
  flightFuel,
  blockFuel,
}: SegmentTableProps) {
  return (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
        <h2 className="text-xl font-semibold text-gray-900">
          Flight Segments
        </h2>
        <p className="text-sm text-gray-600 mt-1">
          Detailed breakdown by flight phase
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Phase
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Duration
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Distance (km)
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Flight Level
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Fuel (kg)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {segments.map((segment, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap">
                  <span className="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800 capitalize">
                    {segment.phase}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {segment.duration}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {segment.distance_km.toLocaleString('en-US', {
                    maximumFractionDigits: 2,
                  })}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  FL{segment.flight_level}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 text-right font-medium">
                  {segment.fuel_kg.toLocaleString('en-US', {
                    maximumFractionDigits: 2,
                  })}
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot className="bg-gray-50 border-t-2 border-gray-300">
            <tr>
              <td
                colSpan={4}
                className="px-6 py-3 text-sm font-semibold text-gray-900 text-right"
              >
                Flight Fuel:
              </td>
              <td className="px-6 py-3 text-sm font-bold text-gray-900 text-right">
                {flightFuel.toLocaleString('en-US', {
                  maximumFractionDigits: 2,
                })}{' '}
                kg
              </td>
            </tr>
            <tr>
              <td
                colSpan={4}
                className="px-6 py-3 text-sm font-semibold text-gray-900 text-right"
              >
                Block Fuel:
              </td>
              <td className="px-6 py-3 text-sm font-bold text-gray-900 text-right">
                {blockFuel.toLocaleString('en-US', {
                  maximumFractionDigits: 2,
                })}{' '}
                kg
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}

