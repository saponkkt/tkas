import React from 'react';
import { FlightAnalysisResult } from '@/services/flightApi';
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';

interface DownloadPanelProps {
  result: FlightAnalysisResult;
}

export default function DownloadPanel({ result }: DownloadPanelProps) {
  const exportToCsv = () => {
    // Create CSV content
    const headers = ['Phase', 'Duration', 'Distance (km)', 'Flight Level', 'Fuel (kg)'];
    const rows = result.segments.map((seg) => [
      seg.phase,
      seg.duration,
      seg.distance_km.toFixed(2),
      `FL${seg.flight_level}`,
      seg.fuel_kg.toFixed(2),
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map((row) => row.join(',')),
      '',
      `Flight Fuel,${result.flight_fuel_kg.toFixed(2)} kg`,
      `Block Fuel,${result.block_fuel_kg.toFixed(2)} kg`,
    ].join('\n');

    // Create and download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `flight-analysis-${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const exportToPdf = () => {
    const doc = new jsPDF();
    const pageWidth = doc.internal.pageSize.getWidth();
    const margin = 20;

    // Title
    doc.setFontSize(18);
    doc.text('Flight Analysis Report', margin, 20);

    // Summary Section
    doc.setFontSize(12);
    let yPos = 35;
    doc.setFont('helvetica', 'bold');
    doc.text('Summary', margin, yPos);
    doc.setFont('helvetica', 'normal');
    yPos += 8;

    const summaryData = [
      ['Total Distance (NM)', result.summary.distance_nm.toFixed(2)],
      ['Total Distance (km)', result.summary.distance_km.toFixed(2)],
      ['Time En-Route', result.summary.time_enroute],
      ['Fuel Consumption (kg)', result.summary.fuel_kg.toFixed(2)],
      ['CO₂ Emissions (kg)', result.summary.co2_kg.toFixed(2)],
      ['Mass Estimate (kg)', result.summary.mass_kg.toFixed(2)],
    ];

    summaryData.forEach(([label, value]) => {
      doc.text(`${label}: ${value}`, margin + 5, yPos);
      yPos += 6;
    });

    yPos += 5;

    // Segments Table
    autoTable(doc, {
      startY: yPos,
      head: [['Phase', 'Duration', 'Distance (km)', 'Flight Level', 'Fuel (kg)']],
      body: result.segments.map((seg) => [
        seg.phase,
        seg.duration,
        seg.distance_km.toFixed(2),
        `FL${seg.flight_level}`,
        seg.fuel_kg.toFixed(2),
      ]),
      theme: 'grid',
      headStyles: { fillColor: [59, 130, 246] },
    });

    const finalY = (doc as any).lastAutoTable.finalY || yPos;
    doc.setFont('helvetica', 'bold');
    doc.text(
      `Flight Fuel: ${result.flight_fuel_kg.toFixed(2)} kg`,
      margin,
      finalY + 10
    );
    doc.text(
      `Block Fuel: ${result.block_fuel_kg.toFixed(2)} kg`,
      margin,
      finalY + 16
    );

    // Footer
    doc.setFontSize(8);
    doc.setTextColor(128, 128, 128);
    doc.text(
      `Generated on ${new Date().toLocaleString()}`,
      pageWidth - margin,
      doc.internal.pageSize.getHeight() - 10,
      { align: 'right' }
    );

    doc.save(`flight-analysis-${new Date().toISOString().split('T')[0]}.pdf`);
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
      <h2 className="text-xl font-semibold text-gray-900 mb-4">Export Results</h2>
      <p className="text-sm text-gray-600 mb-4">
        Download flight analysis results in your preferred format
      </p>

      <div className="flex flex-wrap gap-4">
        <button
          onClick={exportToCsv}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
        >
          <svg
            className="w-5 h-5 mr-2"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
            />
          </svg>
          Download CSV
        </button>

        <button
          onClick={exportToPdf}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
        >
          <svg
            className="w-5 h-5 mr-2"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
            />
          </svg>
          Download PDF
        </button>
      </div>
    </div>
  );
}

