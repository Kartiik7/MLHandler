# MLHandler — Automated CSV Data Preprocessing & EDA Tool

MLHandler is an advanced data preprocessing and exploratory data analysis (EDA) tool designed to clean, type-fix, validate, and profile CSV datasets automatically. Using an asynchronous task queue architecture, it ensures that even large datasets are processed seamlessly without blocking the server, providing real-time feedback to the user via WebSockets.

---

## Key Features

- **Asynchronous Processing Pipeline**: Offloads heavy data cleaning, profiling, and validation to Celery workers.
- **Detailed Data Quality Score**: Generates a comprehensive composite quality score based on missing values, duplicate rows, outlier rates, type consistency, and empty columns.
- **Interactive EDA & Visualizations**: Interactive histograms, categorical distribution charts, Pearson correlation heatmaps, and data quality dashboards.
- **Outlier Detection & Removal**: Detects outliers using Interquartile Range (IQR) and allows selective outlier row removal.
- **Dataset Comparison & Deltas**: Compare two processed datasets side-by-side to track changes, row count differences, schema updates, and distribution shifts.
- **Lineage Tracking**: Visualizes the flow and transformations applied to a dataset step-by-step.
- **Flexible Deployment**: Supports local development or one-step containerized deployment via Docker Compose.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Frontend UI** | React 18 (Vite) + Recharts for interactive visualization |
| **Backend API** | FastAPI + Uvicorn |
| **Task Queue** | Celery 5 |
| **Message Broker** | Redis 7 |
| **Data Processing** | Pandas, NumPy |

---

## System Architecture

```
                 React Frontend
                       │
                       │ 1. POST /upload  →  { task_id }   (returns in milliseconds)
                       │ 2. GET  /download/{task_id}  →  cleaned CSV
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

## Getting Started

You can spin up MLHandler using Docker Compose (recommended) or set up the services manually.

### Option 1: Running with Docker Compose (Recommended)

Make sure you have Docker and Docker Compose installed, then run:

```bash
docker compose up --build
```

This starts the entire stack:
- **Frontend App**: [http://localhost:3000](http://localhost:3000)
- **FastAPI backend**: [http://localhost:8000](http://localhost:8000) (Interactive Swagger docs available at [http://localhost:8000/docs](http://localhost:8000/docs))
- **Redis Server**: `localhost:6379`
- **Celery Worker**: Processes background tasks in lockstep with Redis

To tear down the containers and volumes:
```bash
docker compose down -v
```

---

### Option 2: Local Multi-Terminal Setup (Without Docker)

You will need **four terminal instances** running concurrently.

#### Prerequisites

1. **Redis**: Install and run a Redis instance (default port `6379`).
   ```bash
   docker run -p 6379:6379 redis:7-alpine
   ```
2. **Backend Virtual Environment**:
   ```bash
   cd backend
   python -m venv venv
   # On Windows:
   .\venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Frontend Dependencies**:
   ```bash
   cd frontend
   npm install
   ```

#### Execution Commands

- **Terminal 1 (FastAPI Server)**:
  ```bash
  cd backend
  uvicorn app.main:app --reload --port 8000
  ```
- **Terminal 2 (Celery Worker)**:
  ```bash
  cd backend
  # On macOS/Linux:
  celery -A app.celery_app worker --loglevel=info --concurrency=4
  # On Windows (avoids process-forking issues):
  celery -A app.celery_app worker --loglevel=info --concurrency=1 -P solo
  ```
- **Terminal 3 (React Dev Server)**:
  ```bash
  cd frontend
  npm run dev
  ```

---

## API Reference

### HTTP API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/upload` | Accept CSV upload and queue processing. Returns `{ task_id, status: "queued" }`. Accepts optional custom config JSON or pipeline YAML. |
| `GET` | `/download/{task_id}` | Download processed CSV, Parquet, or Excel files. Query param: `format` (`csv`, `parquet`, `excel`). |
| `GET` | `/lineage/{task_id}` | Fetch lineage data showing transformations applied to the dataset. |
| `GET` | `/column-stats/{task_id}` | Retrieve comprehensive summary statistics, outlier metrics, and semantic types for all columns. |
| `GET` | `/histogram/{task_id}/{column}`| Get histogram counts and bin details for a specific numeric column. Query param: `bins` (2-100). |
| `GET` | `/correlation/{task_id}` | Retrieve a Pearson correlation matrix for numeric columns. Query param: `max_columns`. |
| `GET` | `/quality-score/{task_id}` | Retrieve composite and detailed data quality metrics. |
| `GET` | `/datasets` | Scan history and list metadata for all previously processed datasets. |
| `GET` | `/compare/{task_id_a}/{task_id_b}` | Perform a side-by-side comparison of two processed tasks (returns row/col diffs, quality changes, type/value shifts). |
| `POST` | `/api/outliers` | Detect outliers using IQR. Request body: `{"task_id": "..."}`. |
| `POST` | `/api/remove-outliers` | Exclude rows containing outliers. Request body: `{"task_id": "...", "columns": ["col1", "col2"]}`. |
| `GET` | `/download-outlier-cleaned/{task_id}` | Download the CSV dataset generated after outlier removal. |
| `POST` | `/validate-yaml` | Validate a pipeline YAML configuration string. Request body: `{"yaml": "..."}`. |
| `GET` | `/health` | Liveness check (returns `{ "status": "ok" }`). |

### WebSocket Endpoints

| Endpoint | Protocol | Description |
|---|---|---|
| `/ws/{task_id}` | WebSocket | Streams real-time progress updates: `{ percent, stage, status }` for a running pipeline. |

---

## Project Structure

```
MLHandler/
├── backend/
│   ├── app/
│   │   ├── main.py                # FastAPI application & entry point
│   │   ├── celery_app.py          # Celery configuration and Redis connection
│   │   ├── tasks.py               # process_csv_task (Async pipeline execution wrapper)
│   │   ├── api/
│   │   │   └── routes.py          # HTTP & WebSocket endpoints definition
│   │   ├── services/
│   │   │   ├── pipeline.py        # Orchestrates sequential stages of data processing
│   │   │   ├── pipeline_config.py # Parsers and validators for YAML configuration
│   │   │   ├── loader.py          # CSV loading and basic encoding detection
│   │   │   ├── field_mapper.py    # Maps fields and custom schemas
│   │   │   ├── semantic_inferencer.py # Infers semantic types (email, phone, category, etc.)
│   │   │   ├── type_fixer.py      # Data type correction and coercion rules
│   │   │   ├── cleaner.py         # Imputation, deduplication, and string cleaning
│   │   │   ├── validator.py       # Data validation checks against custom rules
│   │   │   ├── outlier_detector.py # IQR outlier detection & removal algorithms
│   │   │   ├── profiler.py        # Computes descriptive statistics for numeric & categorical columns
│   │   │   ├── reporter.py        # Generates detailed JSON execution and EDA report
│   │   │   └── lineage.py         # Records structural transformations & audit trails
│   │   └── core/
│   │       ├── config.py          # Application configuration settings
│   │       └── logger.py          # Structured logging utilities
│   ├── downloads/                 # Cleaned datasets and report files (Git-ignored)
│   └── requirements.txt           # Python backend dependencies
└── frontend/
    ├── src/
    │   ├── main.jsx               # React client entry point
    │   ├── App.jsx                # Main dashboard page layout and UI orchestrator
    │   ├── hooks/
    │   │   └── useTaskProgress.js # Custom hook for real-time WebSocket connection
    │   └── components/
    │       ├── ProgressBar.jsx           # Clean processing progress indicator
    │       ├── PipelineConfig.jsx        # Pipeline options and YAML validator view
    │       ├── DataQualityDashboard.jsx  # Visual breakdown of scores, outliers, and duplicates
    │       ├── DataVisualizations.jsx    # Numerical distribution charts and value counts
    │       ├── CorrelationHeatmap.jsx    # Interactive Pearson correlation matrix viewer
    │       ├── LineageViewer.jsx         # Step-by-step dataset transformation timeline
    │       └── DatasetComparison.jsx     # Side-by-side comparative dashboard
    └── README.md                  # Frontend-specific setup and dependencies
```

