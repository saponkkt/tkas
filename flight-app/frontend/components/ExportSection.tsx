'use client';

interface ExportSectionProps {
  onExport: () => void;
  disabled?: boolean;
}

export default function ExportSection({ onExport, disabled }: ExportSectionProps) {
  return (
    <div className="bg-white rounded-xl shadow-sm p-6">
      <div className="flex justify-between items-center">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">Export</h3>
          <p className="text-sm text-gray-500 mt-0.5">
            Download processed flight data as CSV
          </p>
        </div>
        <button
        onClick={onExport}
        disabled={disabled}
        className="inline-flex items-center gap-2 px-4 py-2 bg-green-600 text-white font-medium rounded-lg hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        <svg
          className="w-5 h-5"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
          />
        </svg>
        Download processed CSV
        </button>
      </div>
    </div>
  );
}
