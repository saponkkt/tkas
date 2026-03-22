'use client';

import React, { useEffect, useMemo } from 'react';
import {
  MapContainer,
  TileLayer,
  Polyline,
  CircleMarker,
  Popup,
  useMap,
} from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

delete (L.Icon.Default.prototype as unknown as { _getIconUrl?: unknown })._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: '/leaflet/marker-icon-2x.png',
  iconUrl: '/leaflet/marker-icon.png',
  shadowUrl: '/leaflet/marker-shadow.png',
});

interface BoundsFitterProps {
  positions: [number, number][];
}

function BoundsFitter({ positions }: BoundsFitterProps) {
  const map = useMap();
  useEffect(() => {
    if (positions.length > 0) {
      const bounds = L.latLngBounds(positions);
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [40, 40] });
      }
    }
  }, [positions, map]);
  return null;
}

interface FlightMapProps {
  trackPoints: Array<{ lat: number; lon: number }>;
}

export default function FlightMap({ trackPoints }: FlightMapProps) {
  const positions = useMemo(
    () => trackPoints.map((p) => [p.lat, p.lon] as [number, number]),
    [trackPoints]
  );

  const displacementLine = useMemo((): [number, number][] => {
    if (trackPoints.length < 2) return [];
    const first = trackPoints[0];
    const last = trackPoints[trackPoints.length - 1];
    return [
      [first.lat, first.lon],
      [last.lat, last.lon],
    ];
  }, [trackPoints]);

  if (!trackPoints || trackPoints.length === 0) {
    return (
      <div className="h-full min-h-[400px] bg-white rounded-xl overflow-hidden shadow-sm flex items-center justify-center">
        <p className="text-gray-500">No flight track data available</p>
      </div>
    );
  }

  const center = positions[Math.floor(positions.length / 2)];
  const start = trackPoints[0];
  const end = trackPoints[trackPoints.length - 1];

  return (
    <div className="h-full min-h-[400px] relative rounded-xl overflow-hidden shadow-sm">
      <MapContainer
        center={[center[0], center[1]]}
        zoom={6}
        style={{ height: '100%', minHeight: '400px', width: '100%' }}
        className="z-0"
      >
        <TileLayer
          attribution="© OpenStreetMap © CARTO"
          url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png"
        />
        <BoundsFitter positions={positions} />
        <Polyline
          positions={positions}
          pathOptions={{ color: '#1d4ed8', weight: 2.5, opacity: 0.9 }}
        />
        <Polyline
          positions={displacementLine}
          pathOptions={{
            color: '#60a5fa',
            weight: 2,
            dashArray: '10 6',
            opacity: 0.75,
          }}
        />
        <CircleMarker
          center={[start.lat, start.lon]}
          radius={8}
          pathOptions={{
            color: '#16a34a',
            fillColor: '#16a34a',
            fillOpacity: 1,
            weight: 2,
          }}
        >
          <Popup>
            Departure<br />
            {start.lat.toFixed(4)}, {start.lon.toFixed(4)}
          </Popup>
        </CircleMarker>
        <CircleMarker
          center={[end.lat, end.lon]}
          radius={8}
          pathOptions={{
            color: '#dc2626',
            fillColor: '#dc2626',
            fillOpacity: 1,
            weight: 2,
          }}
        >
          <Popup>
            Arrival<br />
            {end.lat.toFixed(4)}, {end.lon.toFixed(4)}
          </Popup>
        </CircleMarker>
      </MapContainer>
      <div
        className="absolute z-[1000] bg-white/90 border border-gray-200 rounded-lg px-3 py-2 text-gray-700 text-xs shadow-sm space-y-0.5"
        style={{ bottom: 12, left: 12 }}
      >
        <div className="flex items-center gap-2">
          <span
            className="w-2 h-2 rounded-full shrink-0"
            style={{ backgroundColor: '#16a34a' }}
          />
          <span>DEPARTURE</span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="w-2 h-2 rounded-full shrink-0"
            style={{ backgroundColor: '#dc2626' }}
          />
          <span>ARRIVAL</span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="w-3 h-0.5 shrink-0"
            style={{ backgroundColor: '#1d4ed8', opacity: 0.9 }}
          />
          <span>FLIGHT PATH</span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className="w-3 h-0.5 shrink-0 border-t-2 border-dashed"
            style={{ borderColor: '#60a5fa', opacity: 0.75 }}
          />
          <span>DISPLACEMENT</span>
        </div>
      </div>
    </div>
  );
}
