## Flight App – FlightRadar24 CSV Analyzer

เว็บแอป **full-stack** สำหรับวิเคราะห์ข้อมูลเที่ยวบินจากไฟล์ CSV (เช่น export จาก FlightRadar24) และคำนวณ:

- **ระยะทาง Great Circle (NM)** ด้วย Haversine formula  
- **Fuel consumption (kg)** = distance\_nm × 4.2  
- **Mass estimate (kg)** = fuel × 1.05  
- **CO₂ emissions (kg)** = fuel × 3.16  

พร้อมทั้งแสดง **เส้นทางการบินบนแผนที่** และตัวอย่าง **n8n workflow** สำหรับเชื่อมอัตโนมัติ

---

## โครงสร้างโปรเจค

- `backend/` – FastAPI + Pandas API สำหรับคำนวณ
- `frontend/` – Next.js + React + Tailwind CSS + Leaflet UI
- `n8n/` – ไฟล์ workflow JSON สำหรับ import เข้า n8n
- `docker-compose.yaml` – รัน backend + n8n ด้วย Docker
- `.gitignore` – ignore สำหรับ Node.js + Python

---

## Backend (FastAPI + Pandas)

### ติดตั้งและรันแบบ local

```bash
cd flight-app/backend
python -m venv venv
venv\Scripts\activate  # บน Windows
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### วิธีรัน backend
```bash
# check directory ก่อนตลอดเด้อ ~/Desktop/Uni/SKATS/skats/
cd flight-app/backend

# เพื่อ setup environment python ให้สามารถหา library เจอได้
source venv/Scripts/activate 

# run project
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
API จะอยู่ที่ `http://localhost:8000`

### Pipeline API (ADS-B + SQLite) – สำหรับ Frontend prototype
Frontend ใหม่ใช้ **Pipeline API** ที่รัน `process_adsb_pipeline.py` และเก็บผลใน SQLite:

```bash
cd flight-app/backend
source venv/Scripts/activate   # หรือ venv\Scripts\activate บน Windows
uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

- **POST /calculate** – อัปโหลด CSV + `aircraft_type` → รัน pipeline → คืนค่า `run_id`
- **GET /summary/{run_id}**, **/track/{run_id}**, **/segments/{run_id}** – ดึงผลจาก SQLite
- **GET /download/csv/{run_id}** – ดาวน์โหลดไฟล์ output CSV จาก pipeline

Database: `backend/flights.db` (SQLite)

### API Spec – POST `/upload` (simple calculator)

- **URL**: `/upload`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`
- **Field**: `file` (CSV)

**CSV columns ที่ต้องมี**

- `lat` – latitude (decimal degrees)
- `lon` – longitude (decimal degrees)
- `altitude` – altitude (ไม่ถูกใช้ในการคำนวณตอนนี้ แต่ต้องมี)
- `timestamp` – เวลา (อ่านด้วย `pandas.to_datetime`)

**ตัวอย่าง cURL**

```bash
curl -X POST "http://localhost:8000/upload" ^
  -H "accept: application/json" ^
  -H "Content-Type: multipart/form-data" ^
  -F "file=@flight.csv;type=text/csv"
```

**ตัวอย่างผลลัพธ์ JSON**

```json
{
  "distance_nm": 523.417,
  "fuel_kg": 2198.351,
  "mass_kg": 2308.269,
  "co2_kg": 6947.827
}
```

---

## ตัวอย่าง CSV Format

ตัวอย่างไฟล์ `flight.csv`:

```csv
lat,lon,altitude,timestamp
13.9125,100.6067,500,2024-08-01T10:00:00Z
14.1000,100.8000,10000,2024-08-01T10:15:00Z
15.0000,101.5000,32000,2024-08-01T11:00:00Z
16.0000,102.5000,34000,2024-08-01T11:45:00Z
```

---

## Frontend (Next.js + Tailwind + Leaflet)

### ติดตั้ง dependencies

```bash
cd flight-app/frontend
npm install
```

### ตั้งค่าเชื่อม backend (ถ้า backend ไม่ใช่ localhost:8000)

สร้างไฟล์ `.env.local` ในโฟลเดอร์ `frontend`:

```env
NEXT_PUBLIC_BACKEND_URL=http://localhost:8000
```

### รัน development server

```bash
npm run dev
```

เปิดเบราว์เซอร์ที่ `http://localhost:3000`

### หน้า `index.js`

- **UploadForm**: ฟอร์มอัปโหลด CSV ไปยัง backend `/upload`
- **ResultsCard**: แสดงผล distance, fuel, mass, CO₂
- **Map**: แผนที่ Leaflet แสดง polyline จากคอลัมน์ `lat` / `lon` ของ CSV
- **Layout**: โครงหน้าเว็บพร้อมปุ่มสลับ **dark/light mode**

---

## n8n Workflow

ในโฟลเดอร์ `n8n/` มีไฟล์:

- `workflow-flight-upload.json` – n8n workflow export

Workflow นี้จะ:

1. รับ Webhook ที่ path `/upload` (ภายใน n8n)  
2. ส่งไฟล์ CSV ไปยัง backend `/upload` (service ชื่อ `backend` ใน Docker network)  
3. เก็บผลลัพธ์ลงฐานข้อมูล Postgres (ต้องตั้งค่า credentials ใน UI)  
4. ส่งอีเมลแจ้งผล (ต้องตั้งค่า SMTP ใน UI)  

### วิธี import workflow

1. เปิด UI ของ n8n (เช่น `http://localhost:5678`)  
2. เลือกเมนู **Import from File**  
3. เลือกไฟล์ `n8n/workflow-flight-upload.json`  
4. ปรับ credentials ของ Postgres / SMTP ตามสภาพแวดล้อมของคุณ  

---

## Docker Compose

ไฟล์ `docker-compose.yaml` จะรัน:

- **backend** – FastAPI service ที่ port `8000`
- **n8n** – n8n automation ที่ port `5678`
- ทั้งสอง service อยู่ใน network: `flight-net`
- volume `n8n_data` ใช้เพื่อ **persist ข้อมูล n8n**

### รันทั้งหมดด้วย Docker

```bash
cd flight-app
docker-compose up -d
```

แล้วคุณจะได้:

- Backend API: `http://localhost:8000`
- n8n UI: `http://localhost:5678`

> หมายเหตุ: frontend (Next.js) แนะนำให้ deploy แยกต่างหาก (เช่น Vercel) หรือรันด้วย `npm run dev`/`npm run start` ภายนอก compose

---

## Deployment Guide (Vercel + Render ฟรี)

### Backend (Render.com – ฟรี tier)

1. Push โค้ดไป GitHub (รวมทั้งโฟลเดอร์ `backend/`)  
2. สร้าง **Web Service** ใหม่ใน Render  
3. เลือก repo และ branch  
4. Root directory: `backend`  
5. Build command:

   ```bash
   pip install -r requirements.txt
   ```

6. Start command:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

7. Deploy แล้วจด **URL** ที่ Render ให้ เช่น `https://flight-backend.onrender.com`  

### Frontend (Vercel)

1. Push โค้ดไป GitHub (รวม `frontend/`)  
2. สร้างโปรเจคใหม่ใน Vercel  
3. เลือก repo และตั้งค่า:
   - Root directory: `frontend`
   - Framework: `Next.js`
4. ตั้งค่า Environment Variable:

   - `NEXT_PUBLIC_BACKEND_URL=https://flight-backend.onrender.com`

5. Deploy และเปิด URL ของ Vercel ที่สร้างให้

---

## Summary

- **Backend**: FastAPI endpoint `/upload` คำนวณ distance, fuel, mass, CO₂ จาก FlightRadar24 CSV  
- **Frontend**: Next.js + Tailwind + Leaflet แสดงผลและเส้นทางการบิน  
- **n8n**: Workflow ตัวอย่าง สำหรับ webhook → เรียก backend → บันทึก DB → ส่งอีเมล  
- **Docker Compose**: รัน backend + n8n บน network `flight-net` พร้อม volume persist  


