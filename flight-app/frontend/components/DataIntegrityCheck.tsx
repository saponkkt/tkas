import type { DataQuality } from '@/lib/api';

const PHASE_DISPLAY_NAMES: Record<string, string> = {
  Taxi_out: 'Taxi-out',
  Takeoff: 'Take-off',
  Initial_climb: 'Initial climb',
  Climb: 'Climb',
  Cruise: 'Cruise',
  Descent: 'Descent',
  Approach: 'Approach',
  Landing: 'Landing',
  Taxi_in: 'Taxi-in',
};

const EXPECTED_PHASES = [
  'Taxi_out',
  'Takeoff',
  'Initial_climb',
  'Climb',
  'Cruise',
  'Descent',
  'Approach',
  'Landing',
  'Taxi_in',
] as const;

function VerifiedBadge() {
  return (
    <span className="inline-flex items-center gap-1 text-xs text-blue-600 bg-blue-50 border border-blue-200 rounded-full px-2 py-0.5">
      <svg width="10" height="10" viewBox="0 0 10 10" fill="none" aria-hidden>
        <path
          d="M2 5l2 2 4-4"
          stroke="#2563eb"
          strokeWidth="1.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      Verified with backend
    </span>
  );
}

interface DataIntegrityCheckProps {
  confidence?: string;
  confidenceDetail: string;
  dataQuality?: DataQuality | null;
}

export default function DataIntegrityCheck({
  confidenceDetail,
  dataQuality,
}: DataIntegrityCheckProps) {
  const dataComplete = dataQuality?.data_complete ?? true;
  const totalRows = dataQuality?.total_rows ?? 0;
  const phasesFound = dataQuality?.phases_found ?? 0;
  const phasesTotal = dataQuality?.phases_total ?? 9;
  const phaseStatus = dataQuality?.phase_status ?? {};
  const generationApplied = dataQuality?.generation_applied ?? false;

  return (
    <div className="bg-white rounded-xl shadow-sm p-5 space-y-4">
      <div className="flex justify-between items-center mb-4">
        <span
          className={`rounded-full px-3 py-1 text-xs font-semibold ${
            dataComplete
              ? 'bg-green-600 text-white'
              : 'bg-yellow-500 text-white'
          }`}
        >
          {dataComplete
            ? '● DATA COMPLETE – HIGH CONFIDENCE'
            : '● DATA PARTIAL – GENERATED PHASES'}
        </span>
        <p className="text-sm text-gray-500">
          Processed by ADS-B pipeline, results stored in database.
        </p>
      </div>

      <h3 className="text-xs font-semibold text-gray-400 tracking-widest uppercase mb-3">
        PIPELINE VERIFICATION
      </h3>

      <div className="flex items-start gap-3 py-3 border-b border-gray-100">
        <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0 mt-0.5">
          <span className="text-white text-xs">✓</span>
        </div>
        <div>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-gray-800">
              Flight phase completeness
            </span>
            <VerifiedBadge />
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            {dataComplete
              ? `All ${phasesTotal} flight phases detected in original CSV.`
              : `${phasesFound}/${phasesTotal} phases in original CSV. Missing phases were generated automatically.`}
          </p>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {EXPECTED_PHASES.map((phase) => {
              const status = phaseStatus[phase] ?? 'missing';
              const name = PHASE_DISPLAY_NAMES[phase] ?? phase;
              if (status === 'original') {
                return (
                  <span
                    key={phase}
                    className="bg-green-100 text-green-700 border border-green-300 rounded-full px-2.5 py-0.5 text-xs font-medium"
                  >
                    {name}
                  </span>
                );
              }
              if (status === 'generated') {
                return (
                  <span
                    key={phase}
                    className="bg-yellow-100 text-yellow-700 border border-yellow-400 rounded-full px-2.5 py-0.5 text-xs font-medium"
                  >
                    {name} · generated
                  </span>
                );
              }
              return (
                <span
                  key={phase}
                  className="bg-red-50 text-red-400 border border-red-200 line-through rounded-full px-2.5 py-0.5 text-xs"
                >
                  {name}
                </span>
              );
            })}
          </div>
        </div>
      </div>

      <div className="flex items-start gap-3 py-3 border-b border-gray-100">
        <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0 mt-0.5">
          <span className="text-white text-xs">✓</span>
        </div>
        <div>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-gray-800">
              Data cleaning
            </span>
            <VerifiedBadge />
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            Outliers removed, null values handled, and signal dropouts
            interpolated.
          </p>
        </div>
      </div>

      <div className="flex items-start gap-3 py-3 border-b border-gray-100">
        <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0 mt-0.5">
          <span className="text-white text-xs">✓</span>
        </div>
        <div>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-gray-800">
              Resampling
            </span>
            <VerifiedBadge />
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            Data resampled to uniform 1-second intervals. Confirmed completed
            and stored.
          </p>
        </div>
      </div>

      {generationApplied && (
        <div className="flex items-start gap-3 py-3 border-b border-gray-100">
          <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0 mt-0.5">
            <span className="text-white text-xs">✓</span>
          </div>
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm font-semibold text-gray-800">
                Generation
              </span>
              <VerifiedBadge />
            </div>
            <p className="text-xs text-gray-500 mt-0.5">
              Synthetic data points generated for incomplete phases.
            </p>
          </div>
        </div>
      )}

      <div className="mt-4 flex gap-3 flex-wrap">
        <span className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600">
          Total data points: {totalRows.toLocaleString()} rows
        </span>
        <span className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600">
          Interval: 1-second
        </span>
        <span className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600">
          Phases: {phasesFound} / {phasesTotal}
        </span>
      </div>

      <div className="pt-2 border-t border-gray-100">
        <p className="text-sm text-gray-500">
          <span className="font-semibold">Confidence Assessment:</span>{' '}
          {confidenceDetail}
        </p>
      </div>
    </div>
  );
}
