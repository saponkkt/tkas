export default function ResultsCard({ results }) {
  if (!results) {
    return (
      <div className="rounded-xl border border-dashed border-slate-700 bg-slate-900/40 p-4 text-sm text-slate-400">
        ผลลัพธ์จะถูกแสดงที่นี่หลังจากอัปโหลดไฟล์ CSV
      </div>
    );
  }

  const { distance_nm, fuel_kg, mass_kg, co2_kg } = results;

  const format = (v) =>
    typeof v === "number" ? v.toLocaleString(undefined, { maximumFractionDigits: 3 }) : v;

  return (
    <div className="grid gap-3 rounded-xl border border-slate-800 bg-slate-900/80 p-4 shadow-lg md:grid-cols-2">
      <Metric
        label="ระยะทางรวม"
        value={`${format(distance_nm)} NM`}
        subtitle="Great-circle distance ที่คำนวณจาก Haversine"
      />
      <Metric
        label="การใช้น้ำมันเชื้อเพลิง"
        value={`${format(fuel_kg)} kg`}
        subtitle="Fuel = distance_nm × 4.2"
      />
      <Metric
        label="ประมาณมวล (Mass estimate)"
        value={`${format(mass_kg)} kg`}
        subtitle="Mass = fuel × 1.05"
      />
      <Metric
        label="การปล่อย CO₂"
        value={`${format(co2_kg)} kg`}
        subtitle="CO₂ = fuel × 3.16"
      />
    </div>
  );
}

function Metric({ label, value, subtitle }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3">
      <div className="text-xs font-medium uppercase tracking-wide text-slate-400">
        {label}
      </div>
      <div className="mt-1 text-lg font-semibold text-emerald-400">{value}</div>
      {subtitle && (
        <div className="mt-1 text-xs text-slate-500">
          {subtitle}
        </div>
      )}
    </div>
  );
}


