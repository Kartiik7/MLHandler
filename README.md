# MLHandler — Automated CSV Data Preprocessing & EDA Tool

MLHandler cleans, type-fixes, and profiles CSV datasets automatically.
It uses an async task architecture so large files never block the server.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Backend API | FastAPI + Uvicorn |
| Task Queue | Celery 5 |
| Message Broker | Redis 7 |
| Data Processing | Pandas, NumPy |
| Frontend | React (Vite) + Recharts |

---

## Architecture

```
React Frontend
    │
    │ 1. POST /upload  →  { task_id }   (returns in milliseconds)
    │ 6. GET  /download/{task_id}  →  cleaned CSV
    │
    ↕  WebSocket /ws/{task_id}
    │  streams: { percent, stage, status }
    │
FastAPI  ──(enqueue)──►  Redis  ──►  Celery Worker
                                         │
                                         ├─ Stage 1 (5%)   Load CSV
                                         ├─ Stage 2 (20%)  Field mapping
                                         ├─ Stage 3 (40%)  Type fixing
                                         ├─ Stage 4 (55%)  Cleaning
                                         ├─ Stage 5 (70%)  Validation
                                         ├─ Stage 6 (80%)  Profiling
                                         ├─ Stage 7 (90%)  Report generation
                                         └─ Stage 8 (95%)  Save outputs
                                              ↓
                                     backend/downloads/{task_id}.csv
                                     backend/downloads/{task_id}_report.json
```

---

## Running the Full System

You need **four terminals** running concurrently.

### Terminal 1 — Redis (via Docker)

```bash
docker run -p 6379:6379 redis:7-alpine
```

> **Alternative (no Docker):** Install Redis natively and run `redis-server`.

### Terminal 2 — FastAPI server

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

### Terminal 3 — Celery worker

```bash
cd backend
celery -A app.celery_app worker --loglevel=info --concurrency=4
```

> On Windows, add `-P solo` if you encounter `forking` errors:
> ```bash
> celery -A app.celery_app worker --loglevel=info --concurrency=1 -P solo
> ```

### Terminal 4 — React dev server

```bash
cd frontend
npm install   # first time only
npm run dev
```

Open **http://localhost:5173** in your browser.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/upload` | Upload CSV. Returns `{ task_id, status: "queued" }` immediately. |
| `GET`  | `/ws/{task_id}` | WebSocket — streams `{ percent, stage, status }` events. |
| `GET`  | `/download/{task_id}` | Download the cleaned CSV once processing completes. |
| `POST` | `/process-csv` | *(Legacy)* Synchronous pipeline — kept for backward compatibility. |
| `GET`  | `/api/outliers` | Detect outliers (IQR) in a CSV. |
| `POST` | `/api/remove-outliers` | Remove outlier rows from selected columns. |
| `GET`  | `/health` | Liveness check — returns `{ status: "ok" }`. |

---

## Installing Backend Dependencies

```bash
cd backend
pip install -r requirements.txt
```

---

## Project Structure

```
MLHandler/
├── backend/
│   ├── app/
│   │   ├── main.py            # FastAPI app + CORS
│   │   ├── celery_app.py      # Celery instance (broker: Redis)
│   │   ├── tasks.py           # process_csv_task (async pipeline)
│   │   ├── api/
│   │   │   └── routes.py      # All HTTP + WebSocket endpoints
│   │   ├── services/
│   │   │   ├── pipeline.py    # Synchronous orchestrator (legacy)
│   │   │   ├── loader.py      # CSV loading
│   │   │   ├── type_fixer.py  # Type inference & fixing
│   │   │   ├── cleaner.py     # Imputation, dedup, whitespace
│   │   │   ├── validator.py   # Schema validation
│   │   │   ├── profiler.py    # EDA profiling
│   │   │   └── reporter.py    # Report generation
│   │   └── core/
│   │       ├── config.py
│   │       └── logger.py
│   ├── downloads/             # Cleaned CSVs & reports (gitignored)
│   └── requirements.txt
└── frontend/
    └── src/
        ├── App.jsx
        ├── hooks/
        │   └── useTaskProgress.js   # WebSocket progress hook
        └── components/
            ├── ProgressBar.jsx      # Animated progress bar
            └── DataVisualizations.jsx
```
