'use client';

import React, { useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Polyline, Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { FlightTrackPoint } from '@/services/flightApi';

// Fix for default marker icons in Next.js
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png',
});

// Custom colored icons using divIcon
const createColoredIcon = (color: string) => {
  return L.divIcon({
    className: 'custom-colored-marker',
    html: `<div style="
      width: 24px;
      height: 24px;
      background-color: ${color};
      border: 3px solid white;
      border-radius: 50%;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    "></div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
  });
};

const startIcon = createColoredIcon('#10b981'); // Green
const endIcon = createColoredIcon('#ef4444'); // Red

interface MapBoundsAdjusterProps {
  bounds: L.LatLngBounds;
}

function MapBoundsAdjuster({ bounds }: MapBoundsAdjusterProps) {
  const map = useMap();
  useEffect(() => {
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [50, 50] });
    }
  }, [bounds, map]);
  return null;
}

interface FlightMapProps {
  track: FlightTrackPoint[];
}

export default function FlightMap({ track }: FlightMapProps) {
  const mapRef = useRef<L.Map | null>(null);

  if (!track || track.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow-md border border-gray-200 h-96 flex items-center justify-center">
        <p className="text-gray-500">No flight track data available</p>
      </div>
    );
  }

  const positions = track.map((point) => [point.lat, point.lon] as [number, number]);
  const bounds = L.latLngBounds(positions);
  const startPoint = track[0];
  const endPoint = track[track.length - 1];

  return (
    <div className="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
        <h2 className="text-xl font-semibold text-gray-900">Flight Track</h2>
        <p className="text-sm text-gray-600 mt-1">Interactive map with ADS-B track visualization</p>
      </div>
      <div className="h-96 w-full relative">
        <MapContainer
          center={[track[Math.floor(track.length / 2)].lat, track[Math.floor(track.length / 2)].lon]}
          zoom={6}
          style={{ height: '100%', width: '100%' }}
          className="z-0"
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapBoundsAdjuster bounds={bounds} />
          <Polyline
            positions={positions}
            pathOptions={{ color: '#3B82F6', weight: 3, opacity: 0.8 }}
            eventHandlers={{
              mouseover: (e) => {
                const layer = e.target;
                layer.setStyle({ weight: 5, color: '#1D4ED8' });
              },
              mouseout: (e) => {
                const layer = e.target;
                layer.setStyle({ weight: 3, color: '#3B82F6' });
              },
            }}
          />
          <Marker position={[startPoint.lat, startPoint.lon]} icon={startIcon}>
            <Popup>
              <div className="text-sm">
                <strong>Departure</strong>
                <br />
                Lat: {startPoint.lat.toFixed(4)}
                <br />
                Lon: {startPoint.lon.toFixed(4)}
                <br />
                Time: {new Date(startPoint.timestamp).toLocaleString()}
              </div>
            </Popup>
          </Marker>
          <Marker position={[endPoint.lat, endPoint.lon]} icon={endIcon}>
            <Popup>
              <div className="text-sm">
                <strong>Arrival</strong>
                <br />
                Lat: {endPoint.lat.toFixed(4)}
                <br />
                Lon: {endPoint.lon.toFixed(4)}
                <br />
                Time: {new Date(endPoint.timestamp).toLocaleString()}
              </div>
            </Popup>
          </Marker>
        </MapContainer>
      </div>
    </div>
  );
}

