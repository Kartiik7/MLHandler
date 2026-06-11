import { useState } from 'react';

// ─────────────────────────────────────────────────────────────────────────────
// Color helpers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Map a Pearson correlation value in [−1, 1] to an RGB color.
 *  +1  →  deep blue   rgb(37, 99, 235)   (blue-600)
 *   0  →  white       rgb(255, 255, 255)
 *  −1  →  deep red    rgb(220, 38, 38)   (red-600)
 *  null→  light gray  rgb(229, 231, 235) (gray-200)
 */
function corrToColor(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 'rgb(229,231,235)';
  }

  const clamped = Math.max(-1, Math.min(1, value));

  if (clamped >= 0) {
    // white → blue
    const t = clamped;
    const r = Math.round(255 + (37 - 255) * t);
    const g = Math.round(255 + (99 - 255) * t);
    const b = Math.round(255 + (235 - 255) * t);
    return `rgb(${r},${g},${b})`;
  } else {
    // white → red
    const t = -clamped;
    const r = Math.round(255 + (220 - 255) * t);
    const g = Math.round(255 + (38 - 255) * t);
    const b = Math.round(255 + (38 - 255) * t);
    return `rgb(${r},${g},${b})`;
  }
}

/**
 * Return 'white' for high-saturation backgrounds so the value text remains
 * legible, and 'rgb(31,41,55)' (gray-800) for light backgrounds.
 */
function labelColor(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return '#6b7280';
  return Math.abs(value) >= 0.55 ? 'white' : 'rgb(31,41,55)';
}

// ─────────────────────────────────────────────────────────────────────────────
// Sub-components
// ─────────────────────────────────────────────────────────────────────────────

/** Gradient legend bar showing the −1 → 0 → +1 color scale. */
function LegendBar() {
  const steps = 40;
  const cellW = 8;
  const totalW = steps * cellW;

  return (
    <div className="flex items-center gap-3 mt-4 select-none">
      <span className="text-xs text-red-600 font-semibold">−1</span>
      <svg width={totalW} height={20} className="rounded overflow-hidden">
        {Array.from({ length: steps }, (_, i) => {
          const val = -1 + (2 * i) / (steps - 1);
          return (
            <rect
              key={i}
              x={i * cellW}
              y={0}
              width={cellW}
              height={20}
              fill={corrToColor(val)}
            />
          );
        })}
      </svg>
      <span className="text-xs text-blue-600 font-semibold">+1</span>
      <span className="text-xs text-gray-400 ml-2">Pearson r</span>
    </div>
  );
}

/** Single heatmap cell with hover tooltip. */
function HeatCell({ value, isDiagonal, col, row, cellSize, fontSize }) {
  const [hovered, setHovered] = useState(false);

  const bg = isDiagonal ? 'rgb(254,243,199)' : corrToColor(value); // amber-100 for diagonal
  const fg = isDiagonal ? 'rgb(120,53,15)' : labelColor(value);    // amber-900 for diagonal text
  const displayVal =
    value === null ? 'N/A' : isDiagonal ? '1.00' : value.toFixed(2);

  return (
    <td
      style={{
        width: cellSize,
        height: cellSize,
        minWidth: cellSize,
        backgroundColor: bg,
        color: fg,
        fontSize,
        fontWeight: isDiagonal ? '700' : '500',
        textAlign: 'center',
        verticalAlign: 'middle',
        cursor: 'default',
        position: 'relative',
        border: '1px solid rgba(0,0,0,0.06)',
        transition: 'filter 0.1s',
        filter: hovered && !isDiagonal ? 'brightness(0.88)' : 'none',
        userSelect: 'none',
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      title={
        isDiagonal
          ? `${col} (self-correlation = 1.00)`
          : value === null
          ? `${row} × ${col}: no data`
          : `${row} × ${col}: ${value.toFixed(4)}`
      }
    >
      {displayVal}
    </td>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

/**
 * CorrelationHeatmap
 *
 * Props:
 *   corrData  — API response: { columns, matrix, sampled, dropped_columns,
 *               row_sampled, sample_rows, total_numeric_columns, method }
 *   loading   — boolean
 *   error     — string | null
 */
export default function CorrelationHeatmap({ corrData, loading, error }) {
  // ── Loading ──────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-500 gap-3">
        <svg
          className="animate-spin h-5 w-5 text-indigo-500"
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          viewBox="0 0 24 24"
        >
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
        </svg>
        <span className="text-sm">Computing correlation matrix…</span>
      </div>
    );
  }

  // ── Error ────────────────────────────────────────────────────────────────
  if (error) {
    return (
      <div className="flex items-center justify-center h-32">
        <p className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-4 py-3">
          {error}
        </p>
      </div>
    );
  }

  // ── No data ──────────────────────────────────────────────────────────────
  if (!corrData || !corrData.columns || corrData.columns.length < 2) {
    return (
      <div className="flex items-center justify-center h-32 text-gray-500 text-sm">
        No correlation data available.
      </div>
    );
  }

  const { columns, matrix, sampled, dropped_columns, row_sampled, sample_rows, total_numeric_columns } = corrData;
  const n = columns.length;

  // ── Adaptive cell sizing based on column count ────────────────────────────
  let cellSize, fontSize, labelMaxLen;
  if (n <= 8) {
    cellSize = 64; fontSize = '12px'; labelMaxLen = 12;
  } else if (n <= 12) {
    cellSize = 52; fontSize = '11px'; labelMaxLen = 10;
  } else if (n <= 18) {
    cellSize = 44; fontSize = '10px'; labelMaxLen = 8;
  } else {
    cellSize = 36; fontSize = '9px';  labelMaxLen = 6;
  }

  /** Truncate long column names for compact display. */
  const truncate = (s) =>
    s.length > labelMaxLen ? s.slice(0, labelMaxLen - 1) + '…' : s;

  // ── Render ───────────────────────────────────────────────────────────────
  return (
    <div className="w-full">
      {/* ── Sampling warnings ─────────────────────────────────────────── */}
      {sampled && (
        <div className="mb-3 flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-4 py-2.5 text-xs text-amber-800">
          <svg className="h-4 w-4 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M8.257 3.099c.366-.756 1.42-.756 1.786 0l5.636 11.637c.344.71-.147 1.514-.893 1.514H3.214c-.747 0-1.237-.804-.893-1.514L8.257 3.099zm.743 2.651a.75.75 0 011.5 0v3.5a.75.75 0 01-1.5 0V5.75zm.75 7.5a.75.75 0 100-1.5.75.75 0 000 1.5z" clipRule="evenodd"/>
          </svg>
          <span>
            Showing top <strong>{n}</strong> of <strong>{total_numeric_columns}</strong> numeric columns
            (ranked by variance). Excluded: <em>{dropped_columns.slice(0, 6).join(', ')}{dropped_columns.length > 6 ? ` +${dropped_columns.length - 6} more` : ''}</em>.
          </span>
        </div>
      )}

      {row_sampled && (
        <div className="mb-3 flex items-start gap-2 bg-blue-50 border border-blue-200 rounded-lg px-4 py-2.5 text-xs text-blue-800">
          <svg className="h-4 w-4 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd"/>
          </svg>
          <span>
            Correlation computed on a <strong>{sample_rows?.toLocaleString()}</strong>-row random sample
            (dataset exceeds 500 000 rows).
          </span>
        </div>
      )}

      {/* ── Scrollable heatmap grid ───────────────────────────────────── */}
      <div className="overflow-x-auto overflow-y-auto" style={{ maxHeight: '600px' }}>
        <table
          style={{
            borderCollapse: 'collapse',
            tableLayout: 'fixed',
            minWidth: (n + 1) * cellSize,
          }}
        >
          {/* Column header row */}
          <thead>
            <tr>
              {/* top-left empty corner */}
              <th style={{ width: cellSize, minWidth: cellSize }} />
              {columns.map((col) => (
                <th
                  key={col}
                  title={col}
                  style={{
                    width: cellSize,
                    minWidth: cellSize,
                    height: cellSize + 24,
                    verticalAlign: 'bottom',
                    paddingBottom: 6,
                    fontWeight: '600',
                    fontSize,
                    color: '#374151',
                    whiteSpace: 'nowrap',
                    textAlign: 'center',
                  }}
                >
                  {/* Rotate label 45° for readability */}
                  <div
                    style={{
                      display: 'inline-block',
                      transform: 'rotate(-45deg)',
                      transformOrigin: 'bottom left',
                      marginLeft: cellSize * 0.3,
                      maxWidth: cellSize * 2.5,
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}
                  >
                    {truncate(col)}
                  </div>
                </th>
              ))}
            </tr>
          </thead>

          {/* Data rows */}
          <tbody>
            {columns.map((rowCol, rowIdx) => (
              <tr key={rowCol}>
                {/* Row label */}
                <td
                  title={rowCol}
                  style={{
                    width: cellSize,
                    minWidth: cellSize,
                    height: cellSize,
                    fontSize,
                    fontWeight: '600',
                    color: '#374151',
                    paddingRight: 8,
                    textAlign: 'right',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    maxWidth: cellSize,
                    verticalAlign: 'middle',
                  }}
                >
                  {truncate(rowCol)}
                </td>

                {/* Correlation cells */}
                {columns.map((colCol, colIdx) => (
                  <HeatCell
                    key={colCol}
                    value={matrix[rowIdx]?.[colIdx] ?? null}
                    isDiagonal={rowIdx === colIdx}
                    row={rowCol}
                    col={colCol}
                    cellSize={cellSize}
                    fontSize={fontSize}
                  />
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── Legend ───────────────────────────────────────────────────────── */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mt-4 gap-3">
        <LegendBar />
        <div className="flex items-center gap-3 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <span
              className="inline-block w-4 h-4 rounded-sm border border-amber-300"
              style={{ backgroundColor: 'rgb(254,243,199)' }}
            />
            Self (1.00)
          </span>
          <span className="flex items-center gap-1">
            <span
              className="inline-block w-4 h-4 rounded-sm border border-gray-200"
              style={{ backgroundColor: 'rgb(229,231,235)' }}
            />
            N/A (constant)
          </span>
          <span className="uppercase tracking-wide font-medium">
            Method: Pearson
          </span>
        </div>
      </div>
    </div>
  );
}
