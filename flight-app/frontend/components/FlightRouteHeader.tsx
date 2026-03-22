import type { AirportInfo, RouteInfo } from '@/lib/api';

function formatCoords(lat?: number, lon?: number): string {
  if (lat == null || lon == null || Number.isNaN(lat) || Number.isNaN(lon)) {
    return '';
  }
  return `${lat.toFixed(2)}°, ${lon.toFixed(2)}°`;
}

/** Line 3: both codes as ICAO / IATA, or a single code */
function formatIcaoIataLine(icao?: string, iata?: string): string {
  const ic = icao?.trim();
  const ia = iata?.trim();
  if (ic && ia) return `${ic} / ${ia}`;
  if (ic) return ic;
  if (ia) return ia;
  return '';
}

type TerminusLines = {
  line1: string;
  line2: string;
  line3: string;
};

/** Only treat as an airport title if the string names an aviation facility (not e.g. a city from geocoding). */
function looksLikeAirportName(name: string): boolean {
  const n = name.trim();
  if (!n) return false;
  return /\b(airport|aerodrome|heliport|airfield|air\s+base|afb)\b/i.test(n);
}

function terminusFromAirport(info: AirportInfo | undefined): TerminusLines {
  if (!info) {
    return { line1: '—', line2: '', line3: '—' };
  }

  const city = info.city?.trim() ?? '';
  const countryCode = info.country_code?.trim().toUpperCase() ?? '';

  let line1 = '';
  if (city && countryCode) {
    line1 = `${city}, ${countryCode}`;
  } else if (city) {
    line1 = city;
  } else if (countryCode) {
    line1 = countryCode;
  } else {
    line1 = formatCoords(info.lat, info.lon) || '—';
  }

  const rawName = info.name?.trim() ?? '';
  const line2 = looksLikeAirportName(rawName) ? rawName : '';

  const codes = formatIcaoIataLine(info.icao, info.iata);
  const line3 = codes || '—';

  return { line1, line2, line3 };
}

function aircraftLabel(aircraftType: string): string {
  const t = aircraftType?.trim() || '';
  const lower = t.toLowerCase();
  if (lower === '737' || lower === 'boeing 737') return 'Boeing 737';
  if (
    lower === '320' ||
    lower === 'a320' ||
    lower === 'airbus 320' ||
    lower === 'airbus a320'
  ) {
    return 'Airbus A320';
  }
  return t || '—';
}

function TerminusSide({
  lines,
  align,
}: {
  lines: TerminusLines;
  align: 'left' | 'right';
}) {
  const textAlign = align === 'left' ? 'text-left' : 'text-right';
  const hasLine2 = Boolean(lines.line2.trim());

  return (
    <div className={`min-w-0 ${textAlign}`}>
      <p className="text-lg font-semibold text-gray-900 truncate" title={lines.line1}>
        {lines.line1}
      </p>
      {hasLine2 ? (
        <p className="text-base text-gray-800 mt-1 truncate" title={lines.line2}>
          {lines.line2}
        </p>
      ) : null}
      <p
        className={`text-sm text-gray-500 truncate ${hasLine2 ? 'mt-0.5' : 'mt-1'}`}
        title={lines.line3}
      >
        {lines.line3}
      </p>
    </div>
  );
}

interface FlightRouteHeaderProps {
  routeInfo?: RouteInfo | null;
  aircraftType: string;
  flightDate?: string | null;
}

export default function FlightRouteHeader({
  routeInfo,
  aircraftType,
  flightDate,
}: FlightRouteHeaderProps) {
  const dep = terminusFromAirport(routeInfo?.departure);
  const arr = terminusFromAirport(routeInfo?.arrival);
  const dateLine = flightDate?.trim() || '—';

  return (
    <div className="bg-white rounded-xl shadow-sm px-8 py-5 mb-4">
      <div className="grid grid-cols-3 gap-6 items-start">
        <TerminusSide lines={dep} align="left" />

        <div className="text-center min-w-0 flex flex-col items-center pt-0.5">
          <svg
            viewBox="0 0 24 24"
            fill="currentColor"
            className="w-6 h-6 text-gray-400 mb-2 rotate-90"
            aria-hidden
          >
            <path d="M21 16v-2l-8-5V3.5c0-.83-.67-1.5-1.5-1.5S10 2.67 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z" />
          </svg>
          <p className="text-sm font-medium text-gray-800">
            {aircraftLabel(aircraftType)}
          </p>
          <p className="text-xs text-gray-400 mt-1">{dateLine}</p>
        </div>

        <TerminusSide lines={arr} align="right" />
      </div>
    </div>
  );
}
