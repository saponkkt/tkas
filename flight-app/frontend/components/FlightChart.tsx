'use client';

import { useMemo, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import type { ChartData as ChartJsData, Scale } from 'chart.js';
import type { ChartData } from '@/lib/api';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const crosshairPlugin = {
  id: 'crosshair',
  afterDraw(chart: ChartJS) {
    const active = chart.tooltip?.getActiveElements?.() ?? [];
    if (active.length > 0) {
      const x = active[0].element.x;
      const ctx = chart.ctx;
      const yAxis = chart.scales.y;
      if (yAxis) {
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, yAxis.top);
        ctx.lineTo(x, yAxis.bottom);
        ctx.lineWidth = 1;
        ctx.strokeStyle = 'rgba(100,116,139,0.4)';
        ctx.setLineDash([4, 4]);
        ctx.stroke();
        ctx.restore();
      }
    }
  },
};

ChartJS.register(crosshairPlugin);

const METRICS = [
  {
    key: 'altitude_fl',
    label: 'Altitude, FL',
    yAxisLabel: 'Altitude (FL)',
    unit: 'FL',
    color: '#1e3a8a',
  },
  {
    key: 'tas_kt',
    label: 'True airspeed, knots',
    yAxisLabel: 'TAS (knots)',
    unit: 'knots',
    color: '#1e40af',
  },
  {
    key: 'ground_speed_kt',
    label: 'Ground speed, knots',
    yAxisLabel: 'Ground speed (knots)',
    unit: 'knots',
    color: '#1d4ed8',
  },
  {
    key: 'weight_kg',
    label: 'Weight, kg',
    yAxisLabel: 'Weight (kg)',
    unit: 'kg',
    color: '#2563eb',
  },
  {
    key: 'total_fuel_kg',
    label: 'Total fuel, kg',
    yAxisLabel: 'Total fuel (kg)',
    unit: 'kg',
    color: '#0284c7',
  },
  {
    key: 'total_co2_kg',
    label: 'Total CO₂, kg',
    yAxisLabel: 'Total CO₂ (kg)',
    unit: 'kg',
    color: '#075985',
  },
] as const;

type MetricKey = (typeof METRICS)[number]['key'];

interface FlightChartProps {
  data: ChartData;
}

export default function FlightChart({ data }: FlightChartProps) {
  const [metric, setMetric] = useState<MetricKey>('altitude_fl');

  const METRIC_BOUNDS = useMemo(() => {
    if (!data) {
      return {} as Record<string, { min: number; max: number; step: number }>;
    }

    const metricKeys: MetricKey[] = [
      'altitude_fl',
      'tas_kt',
      'ground_speed_kt',
      'weight_kg',
      'total_fuel_kg',
      'total_co2_kg',
    ];

    const bounds: Record<string, { min: number; max: number; step: number }> =
      {};

    for (const key of metricKeys) {
      const series = data[key as keyof ChartData] as number[] | undefined;
      const values = (series ?? []).filter(
        (v): v is number => v != null && isFinite(v)
      );

      if (values.length === 0) {
        bounds[key] = { min: 0, max: 100, step: 20 };
        continue;
      }

      const rawMin = Math.min(...values);
      const rawMax = Math.max(...values);

      const useZeroMin = key !== 'weight_kg';

      let min: number;
      if (useZeroMin) {
        min = 0;
      } else if (rawMin > 0) {
        const magnitude = Math.pow(10, Math.floor(Math.log10(rawMin)));
        min = Math.floor(rawMin / magnitude) * magnitude;
      } else {
        min = 0;
      }

      const range = rawMax - min;
      if (range === 0) {
        bounds[key] = { min, max: min + 100, step: 20 };
        continue;
      }

      const roughStep = range / 5;
      const stepMagnitude = Math.pow(10, Math.floor(Math.log10(roughStep)));
      const ratio = roughStep / stepMagnitude;
      let niceStep: number;
      if (ratio <= 1) niceStep = stepMagnitude;
      else if (ratio <= 2) niceStep = 2 * stepMagnitude;
      else if (ratio <= 5) niceStep = 5 * stepMagnitude;
      else niceStep = 10 * stepMagnitude;

      const max = min + niceStep * Math.ceil((rawMax - min) / niceStep);

      bounds[key] = { min, max, step: niceStep };
    }

    return bounds;
  }, [data]);

  const activeMetric = METRICS.find((m) => m.key === metric);
  const metricColor = activeMetric?.color ?? '#2563eb';
  const currentUnit = activeMetric?.unit ?? '';
  const activeYLabel = activeMetric?.yAxisLabel ?? '';

  const activeBounds = METRIC_BOUNDS[metric] ?? {
    min: 0,
    max: 1000,
    step: 200,
  };

  const chartData = useMemo((): ChartJsData<'line'> => {
    const values = data[metric] ?? [];
    return {
      labels: data.time_labels,
      datasets: [
        {
          label: `${activeMetric?.label ?? metric} (${currentUnit})`,
          data: values,
          borderColor: metricColor,
          backgroundColor: 'transparent',
          borderWidth: 2,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          pointHoverBackgroundColor: metricColor,
          pointHoverBorderColor: '#ffffff',
          pointHoverBorderWidth: 2,
          fill: false,
        },
      ],
    };
  }, [data, metric, metricColor, activeMetric, currentUnit]);

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: {
          top: 10,
          right: 20,
          bottom: 0,
          left: 0,
        },
      },
      interaction: { mode: 'index' as const, intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index' as const,
          intersect: false,
          backgroundColor: '#1f2937',
          titleColor: '#fff',
          bodyColor: '#93c5fd',
          padding: 8,
          borderRadius: 6,
          callbacks: {
            title: (items: { label?: string }[]) => items[0]?.label ?? '',
            label: (item: { parsed: { y: number | null } }) =>
              ` ${Math.round(Number(item.parsed.y ?? 0)).toLocaleString()} ${currentUnit}`,
          },
        },
      },
      scales: {
        x: {
          grid: { color: '#f3f4f6' },
          title: {
            display: true,
            text: 'Flight Time (hh:mm)',
            color: '#9ca3af',
            font: { size: 11 },
            padding: { top: 8 },
          },
          ticks: {
            maxTicksLimit: 12,
            maxRotation: 0,
            padding: 4,
          },
          afterFit: (axis: Scale) => {
            axis.height = 52;
          },
        },
        y: {
          min: activeBounds.min,
          max: activeBounds.max,
          grid: { color: '#f3f4f6' },
          title: {
            display: true,
            text: activeYLabel,
            color: '#9ca3af',
            font: { size: 11 },
          },
          ticks: {
            stepSize: activeBounds.step,
            maxTicksLimit: 6,
            callback: (val: string | number) =>
              Math.round(Number(val)).toLocaleString(),
          },
          afterFit: (axis: Scale) => {
            axis.width = 80;
          },
        },
      },
    }),
    [
      activeBounds.min,
      activeBounds.max,
      activeBounds.step,
      metric,
      activeMetric,
      currentUnit,
      activeYLabel,
    ]
  );

  return (
    <div className="bg-white rounded-xl shadow-sm p-6 mb-4">
      <div className="h-[200px] mb-4">
        <Line data={chartData} options={options} />
      </div>
      <div className="flex flex-wrap justify-center gap-x-6 gap-y-2 mt-4">
        {METRICS.map((opt) => {
          const active = metric === opt.key;
          return (
            <label
              key={opt.key}
              className="flex items-center gap-1.5 cursor-pointer"
            >
              <span
                className="w-3 h-3 rounded-full border-2 flex-shrink-0"
                style={
                  active
                    ? { backgroundColor: opt.color, borderColor: opt.color }
                    : { borderColor: '#d1d5db', backgroundColor: 'white' }
                }
              />
              <span
                className="text-xs"
                style={
                  active
                    ? { color: opt.color, fontWeight: 600 }
                    : { color: '#9ca3af' }
                }
              >
                {opt.label}
              </span>
              <input
                type="radio"
                name="chart-metric"
                value={opt.key}
                checked={active}
                onChange={() => setMetric(opt.key)}
                className="sr-only"
              />
            </label>
          );
        })}
      </div>
    </div>
  );
}
