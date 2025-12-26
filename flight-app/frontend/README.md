# Flight Analysis Tool - Frontend

Professional web-based flight analysis tool frontend, inspired by ICAO ICEC calculator and Aviapages flight route results.

## Tech Stack

- **Next.js 14** (React + TypeScript)
- **Tailwind CSS** - Styling
- **Leaflet + OpenStreetMap** - Interactive maps (free, no Mapbox required)
- **jsPDF + jspdf-autotable** - PDF export functionality

## Features

### User Flow (Single Page Application)

1. **Upload CSV File** - Upload ADS-B CSV export from FlightRadar24
2. **Select Aircraft Type** - Choose between Airbus or Boeing
3. **Calculate** - Process flight data and generate analysis
4. **View Results** - See comprehensive flight analysis on the same page

### Results Display

- **Summary Cards**: Total distance (NM/km), time en-route, fuel consumption, CO₂ emissions, mass estimate
- **Interactive Map**: Leaflet map with flight track polyline, start/end markers, hover tooltips
- **Segment Table**: Detailed breakdown by flight phase (taxi, climb, cruise, descent)
- **Export Options**: Download results as CSV or PDF

## Project Structure

```
frontend/
├── components/
│   ├── AircraftSelector.tsx    # Aircraft type selection (Airbus/Boeing)
│   ├── DownloadPanel.tsx       # CSV/PDF export buttons
│   ├── FlightMap.tsx           # Leaflet map with flight track
│   ├── LoadingOverlay.tsx      # Loading spinner during processing
│   ├── SegmentTable.tsx        # Detailed segment breakdown table
│   ├── SummaryCards.tsx        # Summary metrics cards
│   └── UploadPanel.tsx         # CSV file upload interface
├── mocks/
│   └── sampleResult.json       # Mock data for development
├── pages/
│   ├── _app.tsx                # Next.js app wrapper
│   └── index.tsx               # Main page (single page app)
├── services/
│   └── flightApi.ts            # API adapter (currently uses mock data)
└── styles/
    └── globals.css             # Global Tailwind styles
```

## Setup & Installation

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Start production server
npm start
```

## Mock Data

The frontend currently uses mock data from `mocks/sampleResult.json`. The API adapter in `services/flightApi.ts` is designed to easily switch to a real backend by uncommenting the fetch call and updating the URL.

## Connecting to Backend

To connect to a real backend:

1. Update `services/flightApi.ts`
2. Set environment variable `NEXT_PUBLIC_BACKEND_URL` in `.env.local`
3. Uncomment the axios fetch code and remove the mock data return

Example:
```typescript
const response = await axios.post<FlightAnalysisResult>(
  process.env.NEXT_PUBLIC_BACKEND_URL + '/upload',
  formData
);
return response.data;
```

## CSV Format

Expected CSV columns:
- Timestamp (UTC)
- Callsign
- Position (lat, lon)
- Altitude
- Speed
- Direction

The frontend does not preview CSV content - it directly uploads and processes.

## Design Principles

- **Professional & Academic**: Clean, engineering-focused UI
- **Component-Based**: Modular, reusable components
- **Type-Safe**: Full TypeScript support
- **Performance**: Dynamic imports for Leaflet to avoid SSR issues
- **Accessibility**: Semantic HTML, proper ARIA labels

## Notes

- No authentication required
- No database storage
- No user accounts
- CSV files are processed client-side (or via backend when connected)
- All calculations are performed server-side (when backend is connected)

