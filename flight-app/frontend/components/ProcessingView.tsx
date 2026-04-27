'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const PROCESSING_TIMEOUT_MS = Number.parseInt(
  // 0 = wait indefinitely (no app-level timeout)
  process.env.NEXT_PUBLIC_PROCESSING_TIMEOUT_MS || '0',
  10
);

const STEP_MAP: Record<string, number> = {
  uploading: -1,
  cleaning: 0,
  resampling: 1,
  generating: 2,
  phases: 3,
  wind: 4,
  tas: 5,
  weight: 6,
  fuel: 7,
  co2: 8,
  saving: 9,
  complete: 9,
};

const STEPS = [
  {
    key: 'cleaning',
    pending: 'Clean data',
    active: 'Cleaning data...',
    done: 'Cleaned data',
  },
  {
    key: 'resampling',
    pending: 'Resample data',
    active: 'Resampling data...',
    done: 'Resampled data',
  },
  {
    key: 'generating',
    pending: 'Generate missing data',
    active: 'Generating missing data...',
    done: 'Generated missing data',
  },
  {
    key: 'phases',
    pending: 'Flight Phases Breakdown',
    active: 'Breaking down flight phases...',
    done: 'Broken down flight phases',
  },
  {
    key: 'wind',
    pending: 'Fetch wind data',
    active: 'Fetching wind data...',
    done: 'Fetched wind data',
  },
  {
    key: 'tas',
    pending: 'Calculate True Airspeed',
    active: 'Calculating True Airspeed...',
    done: 'Calculated True Airspeed',
  },
  {
    key: 'weight',
    pending: 'Calculate Aircraft Weight',
    active: 'Calculating Aircraft Weight...',
    done: 'Calculated Aircraft Weight',
  },
  {
    key: 'fuel',
    pending: 'Calculate Fuel Consumption',
    active: 'Calculating Fuel Consumption...',
    done: 'Calculated Fuel Consumption',
  },
  {
    key: 'co2',
    pending: 'Calculate CO₂ Emissions',
    active: 'Calculating CO₂ Emissions...',
    done: 'Calculated CO₂ Emissions',
  },
  {
    key: 'saving',
    pending: 'Save results data',
    active: 'Saving results data...',
    done: 'Saved results data',
  },
] as const;

type StepDef = (typeof STEPS)[number];

function getStepLabel(step: StepDef, index: number, currentStepIdx: number): string {
  if (index < currentStepIdx) return step.done;
  if (index === currentStepIdx) return step.active;
  return step.pending;
}

function stepTextClass(index: number, currentStepIdx: number): string {
  if (index < currentStepIdx) return 'text-gray-400';
  if (index === currentStepIdx) return 'text-blue-600 font-medium';
  return 'text-gray-400';
}

interface ProcessingViewProps {
  runId: string;
  onTimeout?: () => void;
  onError?: (message: string) => void;
}

export default function ProcessingView({ runId, onTimeout, onError }: ProcessingViewProps) {
  const router = useRouter();
  const [step, setStep] = useState<string>('uploading');
  const [progress, setProgress] = useState(0);
  const [message, setMessage] = useState('');
  const [complete, setComplete] = useState(false);

  useEffect(() => {
    const es = new EventSource(`${API_URL}/runs/${runId}/progress`);
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data) as {
          step: string;
          progress?: number;
          message?: string;
        };
        if (d.step === 'error') {
          es.close();
          onError?.(d.message || 'Processing failed. Please try again.');
          return;
        }
        if (d.step === 'complete') {
          es.close();
          setStep('complete');
          setProgress(100);
          setMessage('Analysis complete');
          setComplete(true);
          return;
        }
        const stepIndex = STEP_MAP[d.step] ?? -1;
        if (stepIndex >= 0) setStep(d.step);
        if (d.message) setMessage(d.message);
        if (d.progress != null) setProgress(d.progress);
      } catch {
        // ignore
      }
    };
    // Don't close on transient network/proxy errors; EventSource will auto-reconnect.
    // We'll rely on the overall timeout to fail gracefully.
    es.onerror = () => {
      setMessage((m) => m || 'Connection hiccup… still working.');
    };
    return () => es.close();
  }, [runId, onError]);

  useEffect(() => {
    if (complete) {
      const t = setTimeout(() => {
        router.push(`/result?run_id=${runId}`);
      }, 800);
      return () => clearTimeout(t);
    }
  }, [complete, runId, router]);

  useEffect(() => {
    // Allow disabling app-level timeout by setting NEXT_PUBLIC_PROCESSING_TIMEOUT_MS=0
    if (Number.isFinite(PROCESSING_TIMEOUT_MS) && PROCESSING_TIMEOUT_MS === 0) return;
    const ms =
      Number.isFinite(PROCESSING_TIMEOUT_MS) && PROCESSING_TIMEOUT_MS > 0
        ? PROCESSING_TIMEOUT_MS
        : 1200000;
    const timeout = setTimeout(() => {
      if (!complete) onTimeout?.();
    }, ms);
    return () => clearTimeout(timeout);
  }, [complete, onTimeout]);

  const currentStepIdx = STEP_MAP[step] ?? -1;

  if (complete) {
    return (
      <div className="max-w-md mx-auto mt-16 space-y-8">
        <div className="flex justify-center">
          <svg
            className="w-16 h-16 text-green-600"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
        </div>
        <p className="text-green-600 text-center text-sm">
          Analysis complete ✓
        </p>
        <div className="space-y-2">
          {STEPS.map((s) => (
            <div key={s.key} className="flex items-start gap-3">
              <div className="flex-shrink-0 mt-0.5 w-5 h-5">
                <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center">
                  <span className="text-white text-xs">✓</span>
                </div>
              </div>
              <p className="text-sm text-gray-400">{s.done}</p>
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-md mx-auto mt-16 space-y-8">
      <div className="flex justify-center">
        <svg
          className="w-16 h-16 text-blue-600 animate-fly"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"
          />
        </svg>
      </div>
      <h2 className="text-xl font-semibold text-center text-gray-900">
        Analyzing Flight Data...
      </h2>

      <div className="h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <div
          className="h-full bg-blue-500 transition-all duration-500 rounded-full"
          style={{ width: `${progress}%` }}
        />
      </div>
      <p className="text-xs text-gray-400 text-right mt-1">{progress}%</p>

      <div className="space-y-2">
        {STEPS.map((s, idx) => {
          const status =
            idx < currentStepIdx
              ? 'done'
              : idx === currentStepIdx
                ? 'active'
                : 'pending';
          return (
            <div key={s.key} className="flex items-start gap-3">
              <div className="flex-shrink-0 mt-0.5 w-5 h-5">
                {status === 'done' && (
                  <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center">
                    <span className="text-white text-xs">✓</span>
                  </div>
                )}
                {status === 'active' && (
                  <div className="w-5 h-5 rounded-full border-2 border-blue-500 border-t-transparent animate-spin" />
                )}
                {status === 'pending' && (
                  <div className="w-5 h-5 rounded-full border-2 border-gray-200" />
                )}
              </div>
              <div>
                <p className={`text-sm ${stepTextClass(idx, currentStepIdx)}`}>
                  {getStepLabel(s, idx, currentStepIdx)}
                </p>
                {status === 'active' && message && (
                  <p className="text-xs text-blue-500 ml-0 mt-0.5">{message}</p>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
