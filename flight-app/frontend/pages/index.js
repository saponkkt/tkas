import { useState } from "react";
import Layout from "../components/Layout";
import UploadForm from "../components/UploadForm";
import ResultsCard from "../components/ResultsCard";
import Map from "../components/Map";

export default function Home() {
  const [results, setResults] = useState(null);
  const [path, setPath] = useState([]);

  return (
    <Layout>
      <div className="space-y-6">
        <section>
          <h2 className="text-2xl font-semibold tracking-tight">
            FlightRadar24 CSV Analyzer
          </h2>
          <p className="mt-1 text-sm text-slate-400">
            อัปโหลดข้อมูล flight track เพื่อตรวจสอบระยะทาง การใช้น้ำมันเชื้อเพลิง
            และประมาณการการปล่อย CO₂ พร้อมดูเส้นทางการบินบนแผนที่
          </p>
        </section>

        <section className="grid gap-6 md:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
          <UploadForm onResults={setResults} onPath={setPath} />
          <ResultsCard results={results} />
        </section>

        <section className="space-y-2">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-medium text-slate-200">
              แผนที่เส้นทางการบิน
            </h3>
            <span className="text-xs text-slate-500">
              ข้อมูลจากคอลัมน์ lat / lon
            </span>
          </div>
          <Map path={path} />
        </section>
      </div>
    </Layout>
  );
}


