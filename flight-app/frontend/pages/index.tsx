import React, { useState } from 'react';
import dynamic from 'next/dynamic';
import UploadPanel from '@/components/UploadPanel';
import AircraftSelector from '@/components/AircraftSelector';
import SummaryCards from '@/components/SummaryCards';
import SegmentTable from '@/components/SegmentTable';
import DownloadPanel from '@/components/DownloadPanel';
import LoadingOverlay from '@/components/LoadingOverlay';
import DataIntegrityCard, { ValidationResult } from '@/components/DataIntegrityCard';
import {
  submitFlightCsv,
  fetchSummary,
  fetchTrack,
  fetchSegments,
  trackPointsToMapFormat,
  AircraftType,
  FlightAnalysisResult,
} from '@/services/flightApi';

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
    setResult(null);
    try {
      const { run_id } = await submitFlightCsv(selectedFile, selectedAircraft);

      const [summary, trackRes, segmentsRes] = await Promise.all([
        fetchSummary(run_id),
        fetchTrack(run_id),
        fetchSegments(run_id),
      ]);

      const track = trackPointsToMapFormat(trackRes.points);

      setResult({
        run_id,
        summary,
        track,
        segments: segmentsRes.segments,
        total_fuel_kg: summary.total_fuel_kg,
        trip_fuel_kg: summary.trip_fuel_kg,
      });
    } catch (error) {
      console.error('Error processing flight data:', error);
      alert(error instanceof Error ? error.message : 'Error processing flight data. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setSelectedFile(null);
    setSelectedAircraft(null);
    setResult(null);
  };

  const validationResult: ValidationResult = result
    ? {
        overallStatus: 'complete',
        checks: [
          {
            key: 'pipeline',
            status: 'ok',
            message: 'Processed by ADS-B pipeline; results stored in database.',
          },
        ],
      }
    : { overallStatus: 'complete', checks: [] };

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-3xl font-bold text-gray-900">Flight Analysis Tool</h1>
          <p className="text-sm text-gray-600 mt-1">
            Professional flight performance analysis from ADS-B data
          </p>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {!result ? (
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
          <div className="space-y-6">
            <div className="flex justify-end">
              <button
                onClick={handleReset}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Analyze Another Flight
              </button>
            </div>

            <div>
              <h2 className="text-2xl font-semibold text-gray-900 mb-4">
                Flight Summary
              </h2>
              <SummaryCards summary={result.summary} />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-stretch">
              <div className="flex flex-col">
                <FlightMap track={result.track} />
              </div>
              <div className="flex flex-col">
                <SegmentTable
                  segments={result.segments}
                  totalFuelKg={result.summary.total_fuel_kg}
                />
              </div>
            </div>

            <div className="w-full">
              <DataIntegrityCard validationResult={validationResult} />
            </div>

            <DownloadPanel runId={result.run_id} />
          </div>
        )}
      </main>

      {isLoading && <LoadingOverlay />}
    </div>
  );
}
