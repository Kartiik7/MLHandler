# MLHandler Frontend

React-based frontend for the MLHandler CSV data processing application with interactive data visualizations and pipelines.

## Features

- **CSV Upload & Processing**: Upload CSV files with custom preprocessing parameters or YAML-defined pipelines.
- **Dynamic Task Progress**: Live updates streamed via WebSockets.
- **Data Quality Dashboard**: Displays overall quality score, missing values, duplicates, outliers, and type-coercion logs.
- **Interactive EDA & Visualizations**:
  - Missing values distribution charts.
  - Interactive histograms with adjustable bin sizes.
  - Categorical value distributions with pie/bar charts.
  - Interactive Pearson correlation matrices.
- **Lineage View**: Step-by-step interactive diagram of dataset transformations.
- **Dataset Comparison**: Side-by-side delta visualization of two datasets.
- **Responsive Layout**: Clean desktop and mobile layouts.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start the development server:
```bash
npm run dev
```

The app will run at `http://localhost:3000` (development server).

## Build for Production

```bash
npm run build
```

The compiled static files will be generated in the `dist` directory.

## Core Components

- **App.jsx**: Main dashboard template managing view states, uploading logic, and page sections.
- **PipelineConfig.jsx**: Advanced pipeline options configuration panel and interactive YAML validator.
- **ProgressBar.jsx**: Progress bar with visual stages updating dynamically over WebSocket.
- **DataQualityDashboard.jsx**: Scoring widget breakdown and data-health report.
- **DataVisualizations.jsx**: Interactive histogram selection and categorical charts.
- **CorrelationHeatmap.jsx**: Heatmap depicting Pearson correlation values between numeric columns.
- **LineageViewer.jsx**: Interactive lineage timeline showing transformations.
- **DatasetComparison.jsx**: View comparing two history logs (deltas, shifted types, row differences).

## API & WebSocket Integration

Connections to the backend server (default `http://localhost:8000`):
- `POST /upload`: Upload file and config to initiate processing.
- `WS /ws/{task_id}`: Real-time progress updates.
- `GET /column-stats/{task_id}`: Statistical information for charts.
- `GET /histogram/{task_id}/{column}`: Column histograms.
- `GET /correlation/{task_id}`: Pearson correlation matrix.
- `GET /quality-score/{task_id}`: Sub-scores and composite quality score.
- `GET /datasets`: Historical logs of processed datasets.
- `GET /compare/{task_id_a}/{task_id_b}`: Comparison matrix endpoint.
- `POST /api/remove-outliers`: Outlier removal execution.

## Tech Stack

- React 18
- Vite (fast development build system)
- Recharts (SVG-based data visualization library)
- Axios (HTTP requests)
- Lucide React (icons)

