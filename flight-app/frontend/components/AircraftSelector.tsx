import React from 'react';
import { AircraftType } from '@/services/flightApi';

interface AircraftSelectorProps {
  selectedType: AircraftType | null;
  onSelect: (type: AircraftType) => void;
}

export default function AircraftSelector({
  selectedType,
  onSelect,
}: AircraftSelectorProps) {
  return (
    <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
      <h2 className="text-xl font-semibold text-gray-900 mb-4">
        Aircraft Type
      </h2>
      <p className="text-sm text-gray-600 mb-4">
        Select the aircraft type for accurate fuel calculations
      </p>

      <div className="grid grid-cols-2 gap-4">
        <button
          onClick={() => onSelect('airbus')}
          className={`p-4 rounded-lg border-2 transition-all ${
            selectedType === 'airbus'
              ? 'border-blue-600 bg-blue-50 shadow-md'
              : 'border-gray-200 hover:border-gray-300 bg-white'
          }`}
        >
          <div className="text-center">
            <div className="text-2xl font-bold text-gray-900 mb-1">
              Airbus
            </div>
            <div className="text-xs text-gray-500">
              A320
            </div>
          </div>
        </button>

        <button
          onClick={() => onSelect('boeing')}
          className={`p-4 rounded-lg border-2 transition-all ${
            selectedType === 'boeing'
              ? 'border-blue-600 bg-blue-50 shadow-md'
              : 'border-gray-200 hover:border-gray-300 bg-white'
          }`}
        >
          <div className="text-center">
            <div className="text-2xl font-bold text-gray-900 mb-1">
              Boeing
            </div>
            <div className="text-xs text-gray-500">
              737
            </div>
          </div>
        </button>
      </div>
    </div>
  );
}

