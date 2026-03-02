import React from 'react';
import { FlightSummary } from '@/services/flightApi';

interface SummaryCardsProps {
  summary: FlightSummary;
}

function fmt(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return '—';
  return v.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

export default function SummaryCards({ summary }: SummaryCardsProps) {
  const cards = [
    {
      title: 'ETOW',
      value: `${fmt(summary.etow_kg)} kg`,
      subvalue: 'Estimated take-off weight',
    },
    {
      title: 'Total Fuel',
      value: `${fmt(summary.total_fuel_kg)} kg`,
      subvalue: summary.total_fuel_kg != null ? `${(summary.total_fuel_kg / 1000).toFixed(2)} tonnes` : '—',
    },
    {
      title: 'Trip Fuel',
      value: `${fmt(summary.trip_fuel_kg)} kg`,
      subvalue: 'Takeoff → landing',
    },
    {
      title: 'Total CO₂',
      value: `${fmt(summary.total_co2_kg)} kg`,
      subvalue: summary.total_co2_kg != null ? `${(summary.total_co2_kg / 1000).toFixed(2)} tonnes` : '—',
    },
    {
      title: 'Aircraft Type',
      value: summary.aircraft_type || '—',
      subvalue: `Run: ${summary.run_id.slice(0, 8)}…`,
    },
    {
      title: 'Created (UTC)',
      value: summary.created_at ? new Date(summary.created_at).toLocaleString() : '—',
      subvalue: '',
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {cards.map((card, index) => (
        <div
          key={index}
          className="bg-white rounded-lg shadow-md p-6 border border-gray-200"
        >
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center space-x-2 mb-2">
                <h3 className="text-sm font-medium text-gray-600">
                  {card.title}
                </h3>
              </div>
              <div className="text-2xl font-bold text-gray-900">
                {card.value}
              </div>
              {card.subvalue && (
                <div className="text-xs text-gray-500 mt-1">{card.subvalue}</div>
              )}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
