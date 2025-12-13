import { useState } from "react";
import Papa from "papaparse";

const BACKEND_URL =
  process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

export default function UploadForm({ onResults, onPath }) {
  const [file, setFile] = useState(null);
  const [aircraftType, setAircraftType] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleFileChange = (e) => {
    setError("");
    const f = e.target.files?.[0];
    setFile(f || null);
  };

  const parseCsvForPath = (fileObj) =>
    new Promise((resolve, reject) => {
      Papa.parse(fileObj, {
        header: true,
        dynamicTyping: true,
        complete: (results) => {
          const rows = results.data || [];
          const coords = rows
            .filter((r) => r.lat !== undefined && r.lon !== undefined)
            .map((r) => [Number(r.lat), Number(r.lon)]);
          resolve(coords);
        },
        error: (err) => reject(err),
      });
    });

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!file) {
      setError("กรุณาเลือกไฟล์ CSV ก่อน");
      return;
    }
    if (!aircraftType) {
      setError("กรุณาเลือกประเภทเครื่องบิน");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("aircraft_type", aircraftType);

      const res = await fetch(`${BACKEND_URL}/upload`, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || "อัปโหลดล้มเหลว");
      }

      const json = await res.json();
      onResults?.(json);

      const coords = await parseCsvForPath(file);
      onPath?.(coords);
    } catch (err) {
      console.error(err);
      setError(err.message || "เกิดข้อผิดพลาดไม่ทราบสาเหตุ");
    } finally {
      setLoading(false);
    }
  };

  return (
    <form
      onSubmit={handleSubmit}
      className="space-y-4 rounded-xl border border-slate-800 bg-slate-900/80 p-4 shadow-lg"
    >
      <div>
        <label className="block text-sm font-medium text-slate-200">
          อัปโหลดไฟล์ FlightRadar24 CSV
        </label>
        <input
          type="file"
          accept=".csv,text/csv"
          onChange={handleFileChange}
          className="mt-2 w-full cursor-pointer rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-100 file:mr-3 file:rounded-md file:border-0 file:bg-emerald-500 file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-white hover:file:bg-emerald-600"
        />
        <p className="mt-1 text-xs text-slate-500">
          ต้องมีคอลัมน์: <code>lat</code>, <code>lon</code>,{" "}
          <code>altitude</code>, <code>timestamp</code>
        </p>
      </div>

      <div>
        <label className="block text-sm font-medium text-slate-200">
          ประเภทเครื่องบิน <span className="text-rose-400">*</span>
        </label>
        <select
          value={aircraftType}
          onChange={(e) => setAircraftType(e.target.value)}
          className="mt-2 w-full rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-100 focus:border-emerald-500 focus:outline-none focus:ring-1 focus:ring-emerald-500"
        >
          <option value="">-- เลือกประเภทเครื่องบิน --</option>
          <option value="737">Boeing 737</option>
          <option value="320">Airbus A320</option>
        </select>
      </div>

      {error && (
        <p className="text-sm text-rose-400" role="alert">
          {error}
        </p>
      )}

      <button
        type="submit"
        disabled={loading}
        className="inline-flex items-center rounded-md bg-emerald-500 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-emerald-600 disabled:cursor-not-allowed disabled:bg-emerald-900"
      >
        {loading ? "กำลังประมวลผล..." : "วิเคราะห์เที่ยวบิน"}
      </button>
    </form>
  );
}


