'use client';

import { Suspense, useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import dynamic from 'next/dynamic';
import Link from 'next/link';
import { getRun, exportRun, getChartData, type RunResult } from '@/lib/api';
import { formatDuration, formatNumber, formatTonnes } from '@/lib/formatters';
import FlightRouteHeader from '@/components/FlightRouteHeader';
import MetricCard from '@/components/MetricCard';
import SegmentsTable from '@/components/SegmentsTable';
import DataIntegrityCheck from '@/components/DataIntegrityCheck';
import ExportSection from '@/components/ExportSection';
import SkeletonLoader from '@/components/SkeletonLoader';

const FlightMap = dynamic(() => import('@/components/FlightMap'), { ssr: false });
const FlightChart = dynamic(() => import('@/components/FlightChart'), {
  ssr: false,
});

// 0 = wait indefinitely (no app-level timeout)
const RESULT_WAIT_MS = Number.parseInt(process.env.NEXT_PUBLIC_RESULT_WAIT_MS || '0', 10);
// 0 = wait indefinitely (no app-level timeout)
const CHART_WAIT_MS = Number.parseInt(process.env.NEXT_PUBLIC_CHART_WAIT_MS || '0', 10);
const POLL_INTERVAL_MS = 2000;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ResultContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const runId = searchParams?.get('run_id') ?? '';
  const [run, setRun] = useState<RunResult | null>(null);
  const [chartData, setChartData] = useState<Awaited<ReturnType<typeof getChartData>> | null>(null);
  const [loading, setLoading] = useState(true);
  const [exportError, setExportError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) {
      router.push('/');
      return;
    }
    let cancelled = false;
    setLoading(true);
    setRun(null);
    setChartData(null);

    const deadline =
      Number.isFinite(RESULT_WAIT_MS) && RESULT_WAIT_MS > 0
        ? Date.now() + RESULT_WAIT_MS
        : null;

    (async () => {
      while (!cancelled) {
        try {
          const r = await getRun(runId);
          if (cancelled) return;
          setRun(r);
          return;
        } catch {
          if (deadline != null && Date.now() >= deadline) return;
          await sleep(POLL_INTERVAL_MS);
        }
      }
    })()
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [runId, router]);

  useEffect(() => {
    if (!runId || !run) return;
    let cancelled = false;
    setChartData(null);

    const deadline =
      Number.isFinite(CHART_WAIT_MS) && CHART_WAIT_MS > 0
        ? Date.now() + CHART_WAIT_MS
        : null;

    (async () => {
      while (!cancelled) {
        try {
          const d = await getChartData(runId);
          if (cancelled) return;
          setChartData(d);
          return;
        } catch {
          if (deadline != null && Date.now() >= deadline) return;
          await sleep(POLL_INTERVAL_MS);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [runId, run]);

  const handleExport = () => {
    setExportError(null);
    exportRun(runId).catch((e) =>
      setExportError(e instanceof Error ? e.message : 'Export failed')
    );
  };

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <SkeletonLoader />
      </div>
    );
  }

  if (!run) {
    return (
      <div className="max-w-2xl mx-auto px-4 py-16 text-center space-y-4">
        <h2 className="text-lg font-semibold text-gray-900">Still processing…</h2>
        <p className="text-sm text-gray-600">
          This run is taking longer than usual. Please keep this page open; it will load
          automatically when ready.
        </p>
        <Link
          href="/"
          className="inline-flex items-center px-4 py-2 border border-blue-600 text-blue-600 font-medium rounded-lg hover:bg-blue-50 transition-colors"
        >
          Back to upload
        </Link>
      </div>
    );
  }

  const {
    aircraft_type,
    etow_kg,
    total_fuel_kg,
    trip_fuel_kg,
    total_co2_kg,
    total_distance_km,
    flight_duration_s,
    confidence,
    confidence_detail,
    segments,
    track_points,
  } = run;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-8">
        <div>
          <h1 className="text-xl font-bold text-gray-900">
            Flight Analysis Tool
          </h1>
          <p className="text-sm text-gray-600 mt-1">
            Professional flight performance analysis from ADS-B data
          </p>
        </div>
        <Link
          href="/"
          className="inline-flex items-center px-4 py-2 border border-blue-600 text-blue-600 font-medium rounded-lg hover:bg-blue-50 transition-colors"
        >
          Analyze Another Flight
        </Link>
      </div>

      <FlightRouteHeader
        routeInfo={run.route_info}
        aircraftType={aircraft_type}
        flightDate={run.flight_date}
      />

      <section className="mb-8">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <MetricCard
            title="ETOW"
            value={`${formatNumber(Math.round(etow_kg))} kg`}
            subtitle="Estimated take-off weight"
          />
          <MetricCard
            title="Total Fuel"
            value={`${formatNumber(Math.round(total_fuel_kg))} kg`}
            subtitle={formatTonnes(total_fuel_kg)}
          />
          <MetricCard
            title="Total CO₂"
            value={`${formatNumber(Math.round(total_co2_kg))} kg`}
            subtitle={formatTonnes(total_co2_kg)}
          />
          <MetricCard
            title="Aircraft"
            value={aircraft_type}
            subtitle={`Run: ${runId.slice(0, 8)}...`}
          />
          <MetricCard
            title="Duration"
            value={formatDuration(flight_duration_s)}
            subtitle="Total flight time"
          />
          <MetricCard
            title="Distance"
            value={`${total_distance_km.toFixed(1)} km`}
            subtitle="Total distance"
          />
        </div>
      </section>

      <section className="flex flex-col lg:flex-row gap-4 items-stretch mb-4">
        <div className="w-full lg:w-2/5 flex flex-col min-h-[400px]">
          <FlightMap trackPoints={track_points} />
        </div>
        <div className="w-full lg:w-3/5 flex flex-col">
          <SegmentsTable
            segments={segments}
            tripFuelKg={trip_fuel_kg}
            totalFuelKg={total_fuel_kg}
            totalDistanceKm={total_distance_km}
            flightDurationS={flight_duration_s}
            totalCo2Kg={total_co2_kg}
          />
        </div>
      </section>

      {chartData && (
        <section className="mb-8">
          <FlightChart data={chartData} />
        </section>
      )}

      <section className="mb-8">
        <DataIntegrityCheck
          confidence={confidence}
          confidenceDetail={confidence_detail}
          dataQuality={run.data_quality}
        />
      </section>

      <section>
        {exportError && (
          <p className="text-sm text-red-600 mb-2">{exportError}</p>
        )}
        <ExportSection onExport={handleExport} />
      </section>
    </div>
  );
}

export default function ResultPage() {
  return (
    <Suspense fallback={<div className="max-w-7xl mx-auto px-4 py-8"><SkeletonLoader /></div>}>
      <ResultContent />
    </Suspense>
  );
}
