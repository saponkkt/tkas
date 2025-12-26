import React, { useState } from 'react';
import dynamic from 'next/dynamic';
import UploadPanel from '@/components/UploadPanel';
import AircraftSelector from '@/components/AircraftSelector';
import SummaryCards from '@/components/SummaryCards';
import SegmentTable from '@/components/SegmentTable';
import DownloadPanel from '@/components/DownloadPanel';
import LoadingOverlay from '@/components/LoadingOverlay';
import { submitFlightCsv, AircraftType, FlightAnalysisResult } from '@/services/flightApi';

// Dynamically import FlightMap to avoid SSR issues with Leaflet
const FlightMap = dynamic(() => import('@/components/FlightMap'), {
  ssr: false,
  loading: () => (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 h-96 flex items-center justify-center">
      <p className="text-gray-500">Loading map...</p>
    </div>
  ),
});

export default function Home() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [selectedAircraft, setSelectedAircraft] = useState<AircraftType | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<FlightAnalysisResult | null>(null);

  const handleCalculate = async () => {
    if (!selectedFile) {
      alert('Please select a CSV file');
      return;
    }
    if (!selectedAircraft) {
      alert('Please select an aircraft type');
      return;
    }

    setIsLoading(true);
    try {
      const analysisResult = await submitFlightCsv(selectedFile, selectedAircraft);
      setResult(analysisResult);
    } catch (error) {
      console.error('Error processing flight data:', error);
      alert('Error processing flight data. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setSelectedFile(null);
    setSelectedAircraft(null);
    setResult(null);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-3xl font-bold text-gray-900">Flight Analysis Tool</h1>
          <p className="text-sm text-gray-600 mt-1">
            Professional flight performance analysis from ADS-B data
          </p>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {!result ? (
          // Input Section
          <div className="space-y-6">
            <UploadPanel
              onFileSelect={setSelectedFile}
              selectedFile={selectedFile}
            />

            <AircraftSelector
              selectedType={selectedAircraft}
              onSelect={setSelectedAircraft}
            />

            <div className="flex justify-end">
              <button
                onClick={handleCalculate}
                disabled={!selectedFile || !selectedAircraft || isLoading}
                className="px-6 py-3 bg-blue-600 text-white font-semibold rounded-lg shadow-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
              >
                Calculate
              </button>
            </div>
          </div>
        ) : (
          // Results Section
          <div className="space-y-6">
            {/* Reset Button */}
            <div className="flex justify-end">
              <button
                onClick={handleReset}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Analyze Another Flight
              </button>
            </div>

            {/* Summary Cards */}
            <div>
              <h2 className="text-2xl font-semibold text-gray-900 mb-4">
                Flight Summary
              </h2>
              <SummaryCards summary={result.summary} />
            </div>

            {/* Map and Segments */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div>
                <FlightMap track={result.track} />
              </div>
              <div>
                <SegmentTable
                  segments={result.segments}
                  flightFuel={result.flight_fuel_kg}
                  blockFuel={result.block_fuel_kg}
                />
              </div>
            </div>

            {/* Export Panel */}
            <DownloadPanel result={result} />
          </div>
        )}
      </main>

      {/* Loading Overlay */}
      {isLoading && <LoadingOverlay />}
    </div>
  );
}

