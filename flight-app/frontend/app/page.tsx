'use client';

import { useState, useCallback } from 'react';
import Papa from 'papaparse';
import UploadZone from '@/components/UploadZone';
import ProcessingView from '@/components/ProcessingView';
import Toast from '@/components/Toast';
import { uploadFlight } from '@/lib/api';

const REQUIRED_COLUMNS = [
  'Timestamp',
  'UTC',
  'Callsign',
  'Position',
  'Altitude',
  'Speed',
  'Direction',
];

type PageState = 'upload' | 'submitting' | 'processing' | 'error';

export default function HomePage() {
  const [state, setState] = useState<PageState>('upload');
  const [file, setFile] = useState<File | null>(null);
  const [aircraftType, setAircraftType] = useState<string>('');
  const [runId, setRunId] = useState<string>('');
  const [errors, setErrors] = useState<{ file?: string; aircraft?: string; csv?: string }>({});
  const [submitError, setSubmitError] = useState<string | null>(null);

  const validateFile = useCallback((f: File | null): string | undefined => {
    if (!f) return undefined;
    if (!f.name.toLowerCase().endsWith('.csv')) {
      return 'Please upload a CSV file';
    }
    return undefined;
  }, []);

  const validateAircraft = useCallback((v: string): string | undefined => {
    if (!v) return 'Please select an aircraft type';
    return undefined;
  }, []);

  const validateCsvColumns = useCallback((f: File): Promise<string | undefined> => {
    return new Promise((resolve) => {
      Papa.parse(f, {
        preview: 1,
        complete: (results) => {
          const rows = results.data as string[][];
          if (!rows?.length) {
            resolve('Invalid CSV. Could not read headers.');
            return;
          }
          const headers = (rows[0] ?? []).map((h) => String(h).trim());
          const missing = REQUIRED_COLUMNS.filter(
            (col) => !headers.some((h) => h.toLowerCase() === col.toLowerCase())
          );
          if (missing.length > 0) {
            resolve(
              `Invalid CSV. Required columns: ${REQUIRED_COLUMNS.join(', ')}`
            );
            return;
          }
          resolve(undefined);
        },
      });
    });
  }, []);

  const handleSubmit = async () => {
    const fileErr = validateFile(file);
    const aircraftErr = validateAircraft(aircraftType);
    setErrors({
      file: fileErr,
      aircraft: aircraftErr,
      csv: undefined,
    });
    setSubmitError(null);

    if (fileErr || aircraftErr) return;
    if (!file) return;

    const csvErr = await validateCsvColumns(file);
    if (csvErr) {
      setErrors((e) => ({ ...e, csv: csvErr }));
      return;
    }

    setState('submitting');
    try {
      const aircraftValue =
        aircraftType === 'Boeing 737' ? '737' : aircraftType === 'Airbus 320' ? 'A320' : aircraftType;
      const res = await uploadFlight(file, aircraftValue);
      setRunId(res.run_id);
      setErrors({});
      setState('processing');
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : 'Upload failed');
      setState('error');
    }
  };

  const aircraftOptions = [
    { value: '', label: 'Select aircraft...', disabled: true },
    { value: 'Boeing 737', label: 'Boeing 737', disabled: false },
    { value: 'Airbus 320', label: 'Airbus 320', disabled: false },
  ];

  return (
    <div className="min-h-screen">
      {state === 'submitting' && (
        <div className="max-w-md mx-auto mt-16 flex flex-col items-center gap-4">
          <div className="w-12 h-12 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" />
          <p className="text-gray-600">Uploading and processing flight data...</p>
        </div>
      )}

      {state === 'upload' && (
        <div className="min-h-[calc(100vh-4rem)] bg-[#f0f2f5] flex flex-col items-center justify-center py-12 px-4">
          <div className="w-full max-w-lg bg-white rounded-2xl shadow-md p-8 mb-6">
            <h1 className="text-2xl font-bold text-gray-900">
              Flight Analysis Tool
            </h1>
            <p className="text-gray-600 mt-1 mb-6">
              Upload ADS-B CSV data to analyze flight performance
            </p>

            {submitError && (
              <div className="mb-6">
                <Toast
                  message={submitError}
                  type="error"
                  onDismiss={() => setSubmitError(null)}
                />
              </div>
            )}

            <div className="space-y-6">
              <UploadZone
                file={file}
                onFileSelect={(f) => {
                  setFile(f);
                  setErrors((e) => ({ ...e, file: validateFile(f) }));
                }}
                error={errors.file}
              />

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Aircraft Type
                </label>
                <select
                  value={aircraftType}
                  onChange={(e) => {
                    const val = e.target.value;
                    setAircraftType(val);
                    setErrors((prev) => ({
                      ...prev,
                      aircraft: validateAircraft(val),
                    }));
                  }}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  {aircraftOptions.map((opt) => (
                    <option
                      key={opt.value}
                      value={opt.value}
                      disabled={opt.disabled}
                    >
                      {opt.label}
                    </option>
                  ))}
                </select>
                {errors.aircraft && (
                  <p className="text-sm text-red-600 mt-1">{errors.aircraft}</p>
                )}
              </div>

              {errors.csv && (
                <p className="text-sm text-red-600">{errors.csv}</p>
              )}

              <button
                onClick={handleSubmit}
                disabled={!file || !aircraftType}
                className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-xl py-3 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Analyze Flight
              </button>
            </div>
          </div>

          <div className="w-full max-w-lg grid grid-cols-3 gap-3">
            <div className="bg-white rounded-xl p-4 shadow-sm text-center">
              <div className="text-blue-600 mb-1 flex justify-center">
                <svg
                  className="w-5 h-5 stroke-current"
                  fill="none"
                  viewBox="0 0 24 24"
                  aria-hidden
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M8.111 16.404a5.5 5.5 0 017.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0"
                  />
                </svg>
              </div>
              <p className="text-xs font-medium text-gray-700">ADS-B Compatible</p>
              <p className="text-xs text-gray-400 mt-0.5">FR24 & raw ADS-B CSV</p>
            </div>
            <div className="bg-white rounded-xl p-4 shadow-sm text-center">
              <div className="text-blue-600 mb-1 flex justify-center">
                <svg
                  className="w-5 h-5 stroke-current"
                  fill="none"
                  viewBox="0 0 24 24"
                  aria-hidden
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"
                  />
                </svg>
              </div>
              <p className="text-xs font-medium text-gray-700">9 Flight Phases</p>
              <p className="text-xs text-gray-400 mt-0.5">Full phase breakdown</p>
            </div>
            <div className="bg-white rounded-xl p-4 shadow-sm text-center">
              <div className="text-blue-600 mb-1 flex justify-center">
                <svg
                  className="w-5 h-5 stroke-current"
                  fill="none"
                  viewBox="0 0 24 24"
                  aria-hidden
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M12 22c4-4 6-8 6-12a6 6 0 10-12 0c0 4 2 8 6 12z"
                  />
                </svg>
              </div>
              <p className="text-xs font-medium text-gray-700">Fuel & CO₂</p>
              <p className="text-xs text-gray-400 mt-0.5">Emissions reporting</p>
            </div>
          </div>

          <p className="text-xs text-gray-400 mt-4 text-center max-w-lg px-2">
            Accepts CSV with columns: Timestamp, UTC, Callsign, Position, Altitude,
            Speed, Direction
          </p>
        </div>
      )}

      {state === 'processing' && runId ? (
        <ProcessingView
          runId={runId}
          onTimeout={() => {
            setSubmitError('Processing timed out. Please try again.');
            setState('error');
          }}
          onError={(msg) => {
            setSubmitError(msg);
            setState('error');
          }}
        />
      ) : null}

      {state === 'error' && (
        <div className="max-w-lg mx-auto mt-16 px-4">
          <div className="bg-white rounded-xl shadow-sm p-8">
            {submitError && (
              <Toast
                message={submitError}
                type="error"
                onDismiss={() => setSubmitError(null)}
              />
            )}
            <button
              onClick={() => {
                setState('upload');
                setSubmitError(null);
              }}
              className="mt-4 w-full px-4 py-2 border border-blue-600 text-blue-600 font-medium rounded-xl hover:bg-blue-50 transition-colors"
            >
              Try Again
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
