import React from 'react';

export default function LoadingOverlay() {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
      <div className="bg-white rounded-lg p-8 shadow-xl max-w-md w-full mx-4">
        <div className="flex flex-col items-center space-y-4">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <div className="text-center">
            <h3 className="text-lg font-semibold text-gray-900">
              Processing Flight Data
            </h3>
            <p className="text-sm text-gray-600 mt-1">
              Analyzing ADS-B track and calculating flight metrics...
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

