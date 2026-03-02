import React from 'react';

export type ValidationStatus = 'complete' | 'partial' | 'invalid';
export type CheckStatus = 'ok' | 'warning' | 'error';

export interface ValidationCheck {
  key: string;
  status: CheckStatus;
  message: string;
}

export interface ValidationResult {
  overallStatus: ValidationStatus;
  checks: ValidationCheck[];
}

interface DataIntegrityCardProps {
  validationResult: ValidationResult;
}

export default function DataIntegrityCard({ validationResult }: DataIntegrityCardProps) {
  const { overallStatus, checks } = validationResult;

  // Status badge configuration
  const statusConfig = {
    complete: {
      label: 'Data Complete – High Confidence',
      bgColor: 'bg-green-100',
      textColor: 'text-green-800',
      borderColor: 'border-green-300',
      icon: (
        <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
          <path
            fillRule="evenodd"
            d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
            clipRule="evenodd"
          />
        </svg>
      ),
    },
    partial: {
      label: 'Partial Data – Results May Be Inaccurate',
      bgColor: 'bg-yellow-100',
      textColor: 'text-yellow-800',
      borderColor: 'border-yellow-300',
      icon: (
        <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
          <path
            fillRule="evenodd"
            d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
            clipRule="evenodd"
          />
        </svg>
      ),
    },
    invalid: {
      label: 'Incomplete Data – Calculation Risk',
      bgColor: 'bg-red-100',
      textColor: 'text-red-800',
      borderColor: 'border-red-300',
      icon: (
        <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
          <path
            fillRule="evenodd"
            d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
            clipRule="evenodd"
          />
        </svg>
      ),
    },
  };

  const currentStatus = statusConfig[overallStatus];

  // Icon for each check status
  const getStatusIcon = (status: CheckStatus) => {
    switch (status) {
      case 'ok':
        return (
          <span className="text-green-600 font-bold" aria-label="Pass">
            ✔
          </span>
        );
      case 'warning':
        return (
          <span className="text-yellow-600 font-bold" aria-label="Warning">
            ⚠
          </span>
        );
      case 'error':
        return (
          <span className="text-red-600 font-bold" aria-label="Error">
            ✖
          </span>
        );
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
        <h2 className="text-xl font-semibold text-gray-900">Data Integrity Check</h2>
        <p className="text-sm text-gray-600 mt-1">
          Validation status of uploaded ADS-B / FR24 CSV data
        </p>
      </div>

      {/* Status Badge */}
      <div className="px-6 py-4 border-b border-gray-200">
        <div
          className={`inline-flex items-center space-x-2 px-4 py-2 rounded-lg border-2 ${currentStatus.bgColor} ${currentStatus.textColor} ${currentStatus.borderColor}`}
        >
          {currentStatus.icon}
          <span className="font-semibold text-sm">{currentStatus.label}</span>
        </div>
      </div>

      {/* Validation Checklist */}
      <div className="px-6 py-4 flex-1 overflow-y-auto">
        <div className="space-y-3">
          {checks.map((check) => (
            <div
              key={check.key}
              className="flex items-start space-x-3 py-2 border-b border-gray-100 last:border-b-0"
            >
              <div className="flex-shrink-0 mt-0.5 w-6 text-center">
                {getStatusIcon(check.status)}
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-sm text-gray-900">{check.message}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Confidence Summary */}
      <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
        <div className="text-xs text-gray-700 leading-relaxed">
          <p className="font-semibold text-gray-900 mb-1">Confidence Assessment:</p>
          {overallStatus === 'complete' ? (
            <p>
              All critical data parameters are present and continuous. Fuel consumption and mass
              estimates are calculated with high confidence. Flight phase transitions are
              well-defined, and no significant data gaps detected. Results are suitable for
              operational planning and emissions reporting.
            </p>
          ) : overallStatus === 'partial' ? (
            <p>
              Some data parameters are missing or contain gaps. Fuel and mass calculations may
              incorporate interpolated values or default assumptions. Accuracy is reduced,
              particularly for climb/descent phases where altitude continuity is critical.
              Results should be used with caution and validated against alternative data sources
              when available.
            </p>
          ) : (
            <p>
              Critical data parameters are missing or severely fragmented. Fuel consumption and
              mass estimates are unreliable and should not be used for operational decisions or
              regulatory reporting. Significant interpolation and assumption-based calculations
              are required, resulting in high uncertainty. Recommend reprocessing source data or
              obtaining alternative flight track data.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

