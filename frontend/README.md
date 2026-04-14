# MLHandler Frontend

React-based frontend for the MLHandler CSV data processing application with interactive data visualizations.

## Features

- **CSV Upload & Processing**: Upload CSV files with configurable cleaning options
- **Data Visualizations**: Interactive charts powered by Recharts
  - Missing values bar chart
  - Numeric column histograms (selectable)
  - Categorical distribution charts (pie/bar chart toggle)
- **Responsive Design**: Works on desktop and mobile devices
- **Real-time Processing**: Live feedback during CSV processing

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start development server:
```bash
npm run dev
```

The app will be available at `http://localhost:3000`

## Build for Production

```bash
npm run build
```

The built files will be in the `dist` folder.

## Components

- **App.jsx**: Main application component with upload form and state management
- **DataVisualizations.jsx**: Reusable visualization component that accepts column statistics

## API Integration

The frontend connects to the FastAPI backend running on `http://localhost:8000`:
- `POST /process-csv`: Upload and process CSV files
- `GET /column-stats`: Get column statistics for visualizations

## Technology Stack

- React 18
- Vite (build tool)
- Recharts (data visualization)
- Axios (HTTP client)
