import React from 'react';

interface DownloadPanelProps {
  outputFile: string | null;
}

export default function DownloadPanel({ outputFile }: DownloadPanelProps) {
  const handleDownloadCsv = () => {
    if (!outputFile) return;
    
    // Download the CSV file directly from the backend
    const link = document.createElement('a');
    link.href = `${process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000'}/download/csv?file=${encodeURIComponent(outputFile)}`;
    link.setAttribute('download', 'flight_output.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
      <h2 className="text-xl font-semibold text-gray-900 mb-4">Export</h2>
      <p className="text-sm text-gray-600 mb-4">
        Download the processed CSV produced by the pipeline (ADS-B output with fuel and CO₂ columns).
      </p>

      <div className="flex flex-wrap gap-4">
        <button
          onClick={handleDownloadCsv}
          disabled={!outputFile}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:bg-gray-400 disabled:cursor-not-allowed"
        >
          <svg
            className="w-5 h-5 mr-2"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
            />
          </svg>
          Download processed CSV
        </button>
      </div>
    </div>
  );
}
