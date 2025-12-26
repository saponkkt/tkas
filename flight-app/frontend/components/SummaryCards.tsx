import React from 'react';
import { FlightSummary } from '@/services/flightApi';

interface SummaryCardsProps {
  summary: FlightSummary;
}

export default function SummaryCards({ summary }: SummaryCardsProps) {
  const cards = [
    {
      title: 'Total Distance',
      value: `${summary.distance_nm.toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} NM`,
      subvalue: `(${summary.distance_km.toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} km)`,
    },
    {
      title: 'Time En-Route',
      value: summary.time_enroute,
      subvalue: 'HH:MM:SS',
    },
    {
      title: 'Fuel Consumption',
      value: `${summary.fuel_kg.toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} kg`,
      subvalue: `${(summary.fuel_kg / 1000).toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} tonnes`,
    },
    {
      title: 'CO₂ Emissions',
      value: `${summary.co2_kg.toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} kg`,
      subvalue: `${(summary.co2_kg / 1000).toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} tonnes`,
    },
    {
      title: 'Mass Estimate',
      value: `${summary.mass_kg.toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} kg`,
      subvalue: `${(summary.mass_kg / 1000).toLocaleString('en-US', {
        maximumFractionDigits: 2,
      })} tonnes`,
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
              <div className="text-xs text-gray-500 mt-1">{card.subvalue}</div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

