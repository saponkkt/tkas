# TKAS — เครื่องมือวิเคราะห์เที่ยวบิน (Flight Analysis Tool)

แอปพลิเคชันเว็บแบบ full-stack สำหรับวิเคราะห์ข้อมูล ADS-B จากไฟล์ CSV ช่วยคำนวณเฟสการบิน การใช้เชื้อเพลิง การปล่อย CO₂ แสดงเส้นทางบนแผนที่ และสรุปผลแบบมืออาชีพ

---

## สารบัญ

1. [ความสามารถหลัก](#ความสามารถหลัก)
2. [สถาปัตยกรรมระบบ](#สถาปัตยกรรมระบบ)
3. [ความต้องการของระบบ](#ความต้องการของระบบ)
4. [รันแบบพัฒนาในเครื่อง (Localhost)](#รันแบบพัฒนาในเครื่อง-localhost)
5. [รันด้วย Docker Compose](#รันด้วย-docker-compose)
6. [ตัวแปรสภาพแวดล้อม](#ตัวแปรสภาพแวดล้อม)
7. [รูปแบบไฟล์ CSV ที่รองรับ](#รูปแบบไฟล์-csv-ที่รองรับ)
8. [โครงสร้างโปรเจกต์](#โครงสร้างโปรเจกต์)
9. [API หลัก (Backend)](#api-หลัก-backend)
10. [การแก้ปัญหาเบื้องต้น](#การแก้ปัญหาเบื้องต้น)

---

## ความสามารถหลัก

- **อัปโหลด CSV** — ตรวจสอบคอลัมน์ที่จำเป็น รองรับข้อมูล ADS-B / FlightRadar24 แบบดิบ
- **ประมวลผลเบื้องหลัง** — ทำความสะอาดข้อมูล รีแซมเปิล รันสายไปป์ `process_adsb_pipeline` เก็บผลใน **MongoDB**
- **ความคืบหน้าแบบ SSE** — แสดงขั้นตอนประมวลผลบนหน้าเว็บแบบเรียลไทม์
- **ผลลัพธ์** — ETOW, เชื้อเพลิงรวม/เที่ยว, CO₂, ระยะทาง, ระยะเวลา, ความมั่นใจของข้อมูล
- **เส้นทางบิน** — แผนที่ (Leaflet), กราฟความสูง/ความเร็ว/น้ำหนัก/เชื้อเพลิง/CO₂ (Chart.js)
- **ตารางเฟสการบิน** — 9 เฟสมาตรฐาน พร้อม scaling ให้สอดคล้องกับสรุปรวม
- **ส่วนหัวเส้นทาง (Route header)** — ประมาณสนามบินต้นทาง/ปลายทาง (Overpass + Nominatim), วันที่จาก UTC ใน CSV
- **ตรวจสอบความสมบูรณ์ข้อมูล** — สถานะเฟส (ต้นฉบับ / สร้างเติม / ขาด)

---

## สถาปัตยกรรมระบบ


| ชั้น             | เทคโนโลยี                                                                                             |
| ---------------- | ----------------------------------------------------------------------------------------------------- |
| **Frontend**     | Next.js 14 (App Router), React 18, TypeScript, Tailwind CSS                                           |
| **แผนที่**       | Leaflet + react-leaflet (dynamic import, `ssr: false`)                                                |
| **กราฟ**         | Chart.js + react-chartjs-2                                                                            |
| **Backend**      | FastAPI (ไฟล์หลัก `main.py`), Uvicorn                                                                 |
| **ฐานข้อมูล**    | MongoDB (PyMongo)                                                                                     |
| **ประมวลผล**     | `preprocessing.py`, `process_adsb_pipeline.py`, `flight_phase.py`                                     |
| **API ทางเลือก** | `backend/api/app.py` — รองรับ `/calculate` แบบอัปโหลดแล้วรันสคริปต์ไปป์ไลน์ (ใช้เมื่อ deploy แยก API) |


ข้อมูลรันหนึ่งครั้ง (`run`) เก็บเป็นเอกสารใน `flight_runs` และแถวไทม์ซีรีส์ใน `flight_output_rows` (เมื่อบันทึกผ่าน `save_processed_run`)

---

## ความต้องการของระบบ

- **Python 3.11+**
- **Node.js 18+** และ npm
- **MongoDB** (รันเองหรือผ่าน Docker)
- **Docker Desktop** (ถ้าใช้ Docker Compose ทั้งสแตก)

---

## รันแบบพัฒนาในเครื่อง (Localhost)

### 1) MongoDB

ตัวอย่างรัน MongoDB ด้วย Docker:

```bash
docker run -d -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=flight_admin \
  -e MONGO_INITDB_ROOT_PASSWORD=flight_secret \
  --name flight-mongodb \
  mongo:latest
```

หรือใช้ `docker compose` จากโฟลเดอร์ `flight-app` (ดูด้านล่าง) เฉพาะบริการ `mongodb`

### 2) Backend

```bash
cd flight-app/backend
python -m venv venv

# Windows (PowerShell / CMD)
source venv/Scripts/activate

# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
```

คัดลอกและแก้ไข environment (ถ้ามี):

```bash
copy .env.example .env
# หรือ: cp .env.example .env
```

เริ่มเซิร์ฟเวอร์:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- API: [http://localhost:8000](http://localhost:8000)
- เอกสาร Swagger: [http://localhost:8000/docs](http://localhost:8000/docs)

### 3) Frontend

```bash
cd flight-app/frontend
npm install
copy .env.local.example .env.local
# ตั้งค่า NEXT_PUBLIC_API_URL=http://localhost:8000 (ค่าเริ่มต้นในไฟล์ตัวอย่างมักชี้ไปที่นี่แล้ว)
npm run dev
```

เปิดเบราว์เซอร์: [http://localhost:3000](http://localhost:3000)

---

## รันด้วย Docker Compose

จากโฟลเดอร์ `flight-app`:

```bash
cd flight-app
docker compose up -d
```

บริการที่ได้:


| บริการ            | พอร์ต | คำอธิบาย                                                |
| ----------------- | ----- | ------------------------------------------------------- |
| **frontend**      | 3000  | UI Next.js                                              |
| **backend**       | 8000  | FastAPI (`main:app`)                                    |
| **mongodb**       | 27017 | ฐานข้อมูล                                               |
| **mongo-express** | 8081  | ดูข้อมูล Mongo ผ่านเว็บ (username/password ตาม compose) |


หยุดบริการ:

```bash
docker compose down
```

หมายเหตุ: Frontend ใน container ใช้ `NEXT_PUBLIC_API_URL=http://localhost:8000` — ฝั่งเบราว์เซอร์ของคุณเรียก API ที่เครื่อง host; ถ้า deploy จริงควรชี้ URL ของ API ที่เข้าถึงได้จาก client

---

## ตัวแปรสภาพแวดล้อม

### Backend (`backend/.env` หรือตัวแปรใน shell)


| ตัวแปร      | คำอธิบาย                                                                                  |
| ----------- | ----------------------------------------------------------------------------------------- |
| `MONGO_URI` | URI เชื่อม MongoDB (ค่าเริ่มต้นในโค้ดชี้ `flight_admin` / `flight_secret` / `flight_app`) |
| `MONGO_DB`  | ชื่อฐานข้อมูล                                                                             |


### Frontend (`frontend/.env.local`)


| ตัวแปร                | คำอธิบาย                                       |
| --------------------- | ---------------------------------------------- |
| `NEXT_PUBLIC_API_URL` | URL ของ FastAPI (เช่น `http://localhost:8000`) |


---

## รูปแบบไฟล์ CSV ที่รองรับ

หน้าอัปโหลดตรวจสอบว่ามีหัวคอลัมน์ (ไม่สนตัวพิมพ์เล็กใหญ่) ดังนี้:

- `Timestamp`
- `UTC`
- `Callsign`
- `Position`
- `Altitude`
- `Speed`
- `Direction`

ไฟล์ต้องเป็น `.csv` ข้อมูลใช้ในการประมวลผลต่อใน `preprocessing` และ `process_adsb_pipeline` ตามที่กำหนดในโปรเจกต์

---

## โครงสร้างโปรเจกต์

```
flight-app/
├── backend/
│   ├── main.py                 # FastAPI หลัก — /upload, /runs, SSE, chart-data, export
│   ├── api/
│   │   └── app.py              # FastAPI ทางเลือก — /calculate, รันสายไปป์ subprocess
│   ├── db/
│   │   └── mongo.py            # MongoDB, segments, route_info, flight_date, ฯลฯ
│   ├── flight_phase.py         # ตรวจจับเฟสการบิน
│   ├── preprocessing.py      # ก่อนประมวลผลหลัก
│   ├── process_adsb_pipeline.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env.example
├── frontend/
│   ├── app/                    # Next.js App Router (page, result, layout)
│   ├── components/             # Navbar, UploadZone, FlightMap, FlightChart, ฯลฯ
│   ├── lib/                    # api.ts, formatters
│   ├── package.json
│   ├── Dockerfile
│   └── .env.local.example
├── docker-compose.yaml
└── README.md
```

---

## API หลัก (Backend)

เอกสารแบบโต้ตอบ: `**GET /docs**` (Swagger)


| Method | Endpoint                    | คำอธิบาย                                                                            |
| ------ | --------------------------- | ----------------------------------------------------------------------------------- |
| GET    | `/`                         | ข้อความทดสอบว่า backend ทำงาน                                                       |
| POST   | `/upload`                   | อัปโหลด CSV + `aircraft_type` — คืน `run_id` ทันที ประมวลผลในเธรด                   |
| GET    | `/health`                   | สุขภาพแอป                                                                           |
| GET    | `/health/db`                | สุขภาพการเชื่อม MongoDB                                                             |
| GET    | `/runs`                     | รายการรันทั้งหมด (สรุปย่อ)                                                          |
| GET    | `/runs/{run_id}`            | รายละเอียดรันครบ (segments, track_points, data_quality, route_info, flight_date, …) |
| DELETE | `/runs/{run_id}`            | ลบรันและแถว output ที่เกี่ยวข้อง                                                    |
| GET    | `/runs/{run_id}/chart-data` | ข้อมูลสำหรับกราฟ (เวลา, ความสูง, ความเร็ว, น้ำหนัก, เชื้อเพลิง, CO₂)                |
| GET    | `/runs/{run_id}/export`     | ดาวน์โหลด CSV ผลลัพธ์                                                               |
| GET    | `/runs/{run_id}/progress`   | **Server-Sent Events** — ความคืบหน้าขั้นตอนประมวลผล                                 |


### API ทางเลือก (`backend/api/app.py`)

เมื่อรันแอปจาก `api/app.py` จะมีเส้นทางเช่น `**POST /calculate`** สำหรับอัปโหลดแล้วรัน `process_adsb_pipeline.py` แยกต่างหาก — ใช้เมื่อโครงสร้าง deploy แยก service

---

## การแก้ปัญหาเบื้องต้น

1. **เชื่อม MongoDB ไม่ได้** — ตรวจว่า Mongo รันที่พอร์ต 27017 และ `MONGO_URI` / user-password ตรงกับ `docker-compose` หรือคอนเทนเนอร์ที่ใช้
2. **Frontend เรียก API ไม่ถึง** — ตรวจ `NEXT_PUBLIC_API_URL` และ CORS (backend ตั้ง `allow_origins` แบบเปิดกว้างในโค้ดปัจจุบัน)
3. **อัปโหลดแล้วค้าง** — ดู log ของ `uvicorn` และขั้นตอนใน SSE; สายไปป์อาจใช้เวลานาน (ลม ERA5 ฯลฯ ตามการตั้งค่าใน `process_adsb_pipeline`)
4. **Route header ไม่แสดงเมือง/สนามบิน** — การค้นหาสนามบินใช้บริการภายนอก (Overpass / Nominatim) หาก API ช้าหรือบล็อก อาจได้ข้อมูลว่าง; การบันทึกรันยังทำงานต่อได้
5. **Docker: frontend ไม่เห็น backend** — จากเบราว์เซอร์บนเครื่องคุณ ยังคงใช้ `localhost:8000` เป็นปกติ; ถ้าเรียกจาก container อื่นต้องใช้ชื่อ service / URL ภายใน network

---

## ใบอนุญาตและการมีส่วนร่วม

โปรเจกต์เป็นส่วนหนึ่งของ workspace **TKAS** — ปรับแต่ง README / โค้ดตามนโยบายทีมของคุณ

หากพัฒนาต่อ แนะนำให้รัน `npm run lint` ใน frontend และทดสอบ `/upload` + หน้า `/result?run_id=...` หลังแก้ backend

---

*อัปเดต README ฉบับภาษาไทย — สอดคล้องกับโครงสร้าง `flight-app` ปัจจุบัน*