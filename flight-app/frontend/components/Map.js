import dynamic from "next/dynamic";
import { useMemo } from "react";

const MapContainer = dynamic(
  () => import("react-leaflet").then((m) => m.MapContainer),
  { ssr: false }
);
const TileLayer = dynamic(
  () => import("react-leaflet").then((m) => m.TileLayer),
  { ssr: false }
);
const Polyline = dynamic(
  () => import("react-leaflet").then((m) => m.Polyline),
  { ssr: false }
);

export default function Map({ path }) {
  const hasPath = Array.isArray(path) && path.length > 0;

  const bounds = useMemo(() => {
    if (!hasPath) return null;
    let minLat = 90,
      maxLat = -90,
      minLon = 180,
      maxLon = -180;
    for (const [lat, lon] of path) {
      if (typeof lat !== "number" || typeof lon !== "number") continue;
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
      minLon = Math.min(minLon, lon);
      maxLon = Math.max(maxLon, lon);
    }
    return [
      [minLat, minLon],
      [maxLat, maxLon],
    ];
  }, [path, hasPath]);

  const center = useMemo(() => {
    if (!hasPath) return [13.7563, 100.5018]; // Bangkok default
    const [min, max] = bounds;
    return [(min[0] + max[0]) / 2, (min[1] + max[1]) / 2];
  }, [bounds, hasPath]);

  return (
    <div className="h-80 w-full overflow-hidden rounded-xl border border-slate-800 bg-slate-900/70">
      <MapContainer
        center={center}
        zoom={5}
        style={{ height: "100%", width: "100%" }}
        bounds={hasPath ? bounds : undefined}
        scrollWheelZoom={false}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {hasPath && (
          <Polyline
            positions={path}
            pathOptions={{ color: "#22c55e", weight: 3, opacity: 0.9 }}
          />
        )}
      </MapContainer>
    </div>
  );
}


