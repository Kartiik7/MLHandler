import React, { useMemo, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Cell, Legend, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D', '#FFC658'];

const SEMANTIC_BADGE_CLASSES = {
  EMAIL: 'bg-blue-100 text-blue-800',
  URL: 'bg-blue-100 text-blue-800',
  PHONE: 'bg-blue-100 text-blue-800',
  PHONE_NUMBER: 'bg-blue-100 text-blue-800',
  CURRENCY: 'bg-green-100 text-green-800',
  PERCENTAGE: 'bg-green-100 text-green-800',
  LATITUDE: 'bg-teal-100 text-teal-800',
  LONGITUDE: 'bg-teal-100 text-teal-800',
  DATE_STRING: 'bg-purple-100 text-purple-800',
  CATEGORICAL_LOW: 'bg-orange-100 text-orange-800',
  CATEGORICAL_HIGH: 'bg-orange-100 text-orange-800',
  FREE_TEXT: 'bg-gray-100 text-gray-800',
  NUMERIC_CONTINUOUS: 'bg-indigo-100 text-indigo-800',
  NUMERIC_ID: 'bg-indigo-100 text-indigo-800',
};

function SemanticBadge({ semanticType }) {
  if (!semanticType) return null;
  const colorClass = SEMANTIC_BADGE_CLASSES[semanticType] || 'bg-gray-100 text-gray-800';
  return <span className={`inline-flex px-2 py-1 rounded-full text-[10px] font-semibold uppercase ${colorClass}`}>{semanticType}</span>;
}

const BoxPlotCard = ({ column, stats, semanticType }) => {
  const { min, q1, median, q3, max, outlierCount } = stats;

  return (
    <div className="p-4 border rounded-md shadow-sm bg-white">
      <div className="flex items-center justify-between mb-2 gap-2">
        <h3 className="text-lg font-semibold">{column}</h3>
        <SemanticBadge semanticType={semanticType} />
      </div>
      <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
        <span>Min: {min.toFixed(2)}</span>
        <span>Q1: {q1.toFixed(2)}</span>
        <span>Median: {median.toFixed(2)}</span>
        <span>Q3: {q3.toFixed(2)}</span>
        <span>Max: {max.toFixed(2)}</span>
        <span>Outliers: {outlierCount}</span>
      </div>
    </div>
  );
};

const BoxPlot = ({ data, columnName }) => {
  if (!data) return null;

  const { min, q1, median, q3, max, whiskerLow, whiskerHigh } = data;
  const chartWidth = 640;
  const chartHeight = 300;
  const padding = { top: 20, right: 40, bottom: 60, left: 60 };
  const plotWidth = chartWidth - padding.left - padding.right;
  const plotHeight = chartHeight - padding.top - padding.bottom;

  const visualMin = Math.min(min, whiskerLow);
  const visualMax = Math.max(max, whiskerHigh);
  const dataRange = Math.max(visualMax - visualMin, 1e-9);
  const scale = plotHeight / dataRange;

  const y = (value) => padding.top + plotHeight - (value - visualMin) * scale;

  const yWhiskerLow = y(whiskerLow);
  const yQ1 = y(q1);
  const yMedian = y(median);
  const yQ3 = y(q3);
  const yWhiskerHigh = y(whiskerHigh);

  const boxLeft = padding.left + plotWidth * 0.25;
  const boxRight = padding.left + plotWidth * 0.75;
  const boxCenter = (boxLeft + boxRight) / 2;

  return (
    <svg width={chartWidth} height={chartHeight} className="mx-auto">
      <line x1={padding.left} y1={padding.top} x2={padding.left} y2={padding.top + plotHeight} stroke="#666" strokeWidth="2" />
      <line
        x1={padding.left}
        y1={padding.top + plotHeight}
        x2={padding.left + plotWidth}
        y2={padding.top + plotHeight}
        stroke="#666"
        strokeWidth="2"
      />

      <line x1={boxCenter} y1={yWhiskerHigh} x2={boxCenter} y2={yQ3} stroke="#4ECDC4" strokeWidth="2" />
      <line x1={boxCenter} y1={yQ1} x2={boxCenter} y2={yWhiskerLow} stroke="#4ECDC4" strokeWidth="2" />
      <line x1={boxLeft + 30} y1={yWhiskerHigh} x2={boxRight - 30} y2={yWhiskerHigh} stroke="#4ECDC4" strokeWidth="2" />
      <line x1={boxLeft + 30} y1={yWhiskerLow} x2={boxRight - 30} y2={yWhiskerLow} stroke="#4ECDC4" strokeWidth="2" />

      <rect x={boxLeft} y={yQ3} width={boxRight - boxLeft} height={yQ1 - yQ3} fill="#4ECDC4" fillOpacity="0.6" stroke="#2C9AA0" strokeWidth="2" />

      <line x1={boxLeft} y1={yMedian} x2={boxRight} y2={yMedian} stroke="#FF6B6B" strokeWidth="3" />

      <text x={padding.left - 10} y={yWhiskerLow} textAnchor="end" fontSize="12" fill="#666">
        {whiskerLow.toFixed(2)}
      </text>
      <text x={padding.left - 10} y={yQ1} textAnchor="end" fontSize="12" fill="#666">
        {q1.toFixed(2)}
      </text>
      <text x={padding.left - 10} y={yMedian} textAnchor="end" fontSize="12" fill="#FF6B6B" fontWeight="bold">
        {median.toFixed(2)}
      </text>
      <text x={padding.left - 10} y={yQ3} textAnchor="end" fontSize="12" fill="#666">
        {q3.toFixed(2)}
      </text>
      <text x={padding.left - 10} y={yWhiskerHigh} textAnchor="end" fontSize="12" fill="#666">
        {whiskerHigh.toFixed(2)}
      </text>

      <text x={boxCenter} y={chartHeight - 20} textAnchor="middle" fontSize="14" fontWeight="bold" fill="#333">
        {columnName}
      </text>

      <text x={15} y={chartHeight / 2} textAnchor="middle" fontSize="12" fill="#666" transform={`rotate(-90, 15, ${chartHeight / 2})`}>
        Value
      </text>
    </svg>
  );
};

const DataVisualizations = ({ data, report, loading = false, onDetectOutliers, onCleanOutliers }) => {
  const [selectedNumericColumn, setSelectedNumericColumn] = useState('');
  const [selectedCategoricalColumn, setSelectedCategoricalColumn] = useState('');
  const [categoricalChartType, setCategoricalChartType] = useState('pie');
  const [selectedBoxPlotColumn, setSelectedBoxPlotColumn] = useState('');

  const [outlierSectionOpen, setOutlierSectionOpen] = useState(false);
  const [outlierData, setOutlierData] = useState(null);
  const [detectingOutliers, setDetectingOutliers] = useState(false);
  const [outlierError, setOutlierError] = useState('');

  const [removeOutliers, setRemoveOutliers] = useState({});
  const [cleaningOutliers, setCleaningOutliers] = useState(false);
  const [cleaningSuccess, setCleaningSuccess] = useState(null);

  const getSemanticType = (columnName) => data?.columns?.[columnName]?.semantic_type || 'UNKNOWN';

  const toggleOutlierColumn = (col) => {
    setRemoveOutliers((prev) => ({ ...prev, [col]: !prev[col] }));
  };

  const handleCleanOutliers = async () => {
    if (!onCleanOutliers) {
      setOutlierError('Outlier cleaning is not available');
      return;
    }

    const selectedCols = Object.keys(removeOutliers).filter((col) => removeOutliers[col]);

    setCleaningOutliers(true);
    setOutlierError('');
    setCleaningSuccess(null);

    const result = await onCleanOutliers(selectedCols);

    if (result.error) {
      setOutlierError(result.error);
      setCleaningSuccess(null);
    } else if (result.success) {
      setCleaningSuccess(result.stats);
      setOutlierError('');
      setRemoveOutliers({});
    }

    setCleaningOutliers(false);
  };

  const { numericColumns, categoricalColumns, missingValuesData } = useMemo(() => {
    if (!data || !data.columns) {
      return { numericColumns: [], categoricalColumns: [], missingValuesData: [] };
    }

    const numeric = [];
    const categorical = [];
    const missingData = [];

    if (report && report._raw_clean_stats && report._raw_clean_stats.missing_before) {
      const missingBefore = report._raw_clean_stats.missing_before;
      Object.entries(missingBefore).forEach(([columnName, nullCount]) => {
        missingData.push({ name: columnName, nullCount: nullCount || 0 });
      });
    }

    Object.entries(data.columns).forEach(([columnName, stats]) => {
      if (stats.type === 'numeric') {
        numeric.push({ name: columnName, ...stats });
      } else if (stats.type === 'categorical') {
        categorical.push({ name: columnName, ...stats });
      }
    });

    return { numericColumns: numeric, categoricalColumns: categorical, missingValuesData: missingData };
  }, [data, report]);

  React.useEffect(() => {
    if (numericColumns.length > 0 && !selectedNumericColumn) setSelectedNumericColumn(numericColumns[0].name);
    if (categoricalColumns.length > 0 && !selectedCategoricalColumn) setSelectedCategoricalColumn(categoricalColumns[0].name);
    if (numericColumns.length > 0 && !selectedBoxPlotColumn) setSelectedBoxPlotColumn(numericColumns[0].name);
  }, [numericColumns, categoricalColumns, selectedNumericColumn, selectedCategoricalColumn, selectedBoxPlotColumn]);

  const histogramData = useMemo(() => {
    if (!selectedNumericColumn || !data?.columns?.[selectedNumericColumn]) return [];

    const stats = data.columns[selectedNumericColumn];
    if (stats.type !== 'numeric' || stats.min === null || stats.max === null) return [];

    const binCount = 10;
    const range = stats.max - stats.min;
    const binWidth = range / binCount;

    const bins = [];
    for (let i = 0; i < binCount; i += 1) {
      const binStart = stats.min + i * binWidth;
      const binEnd = binStart + binWidth;
      const binCenter = (binStart + binEnd) / 2;

      let frequency = 0;
      if (stats.mean !== null && stats.std !== null && stats.std > 0) {
        const z = (binCenter - stats.mean) / stats.std;
        frequency = Math.exp(-0.5 * z * z) * 100;
      } else {
        frequency = 50;
      }

      bins.push({ range: `${binStart.toFixed(1)}-${binEnd.toFixed(1)}`, frequency: Math.round(frequency), center: binCenter });
    }

    return bins;
  }, [selectedNumericColumn, data]);

  const categoricalData = useMemo(() => {
    if (!selectedCategoricalColumn || !data?.columns?.[selectedCategoricalColumn]) return [];

    const stats = data.columns[selectedCategoricalColumn];
    if (stats.type !== 'categorical' || !stats.top_5_values) return [];

    return stats.top_5_values.map((item, index) => ({ name: item.value, value: item.count, fill: COLORS[index % COLORS.length] }));
  }, [selectedCategoricalColumn, data]);

  const boxPlotData = useMemo(() => {
    if (!selectedBoxPlotColumn || !data?.columns?.[selectedBoxPlotColumn]) return null;

    const colData = data.columns[selectedBoxPlotColumn];
    if (colData.type !== 'numeric' || colData.q1 === null || colData.q3 === null || colData.median === null) return null;

    const q1 = colData.q1;
    const median = colData.median;
    const q3 = colData.q3;
    const whiskerLow = colData.whisker_low;
    const whiskerHigh = colData.whisker_high;
    const outlierCount = colData.outlier_count;

    return {
      min: colData.min,
      q1,
      median,
      q3,
      max: colData.max,
      whiskerLow,
      whiskerHigh,
      outlierCount,
    };
  }, [selectedBoxPlotColumn, data]);

  const allBoxPlotData = useMemo(() => {
    if (!data || !data.columns) return {};

    const boxPlots = {};
    Object.entries(data.columns).forEach(([columnName, colData]) => {
      if (colData.type === 'numeric' && colData.q1 !== null && colData.q3 !== null && colData.median !== null) {
        boxPlots[columnName] = {
          min: colData.min,
          q1: colData.q1,
          median: colData.median,
          q3: colData.q3,
          max: colData.max,
          whiskerLow: colData.whisker_low,
          whiskerHigh: colData.whisker_high,
          outlierCount: colData.outlier_count,
        };
      }
    });

    return boxPlots;
  }, [data]);

  if (loading) {
    return (
      <div className="visualizations-container loading">
        <div className="loading-spinner">
          <div className="spinner" />
          <p>Loading visualizations...</p>
        </div>
      </div>
    );
  }

  if (!data || !data.columns) {
    return (
      <div className="visualizations-container empty">
        <p>No data available for visualization. Please upload and process a CSV file first.</p>
      </div>
    );
  }

  return (
    <div className="visualizations-container">
      <h2>Data Visualizations</h2>

      <div className="stats-overview">
        <div className="stat-card">
          <h4>Total Rows</h4>
          <p className="stat-value">{data.total_rows?.toLocaleString() || 'N/A'}</p>
        </div>
        <div className="stat-card">
          <h4>Total Columns</h4>
          <p className="stat-value">{data.total_columns || 'N/A'}</p>
        </div>
        <div className="stat-card">
          <h4>Numeric Columns</h4>
          <p className="stat-value">{numericColumns.length}</p>
        </div>
        <div className="stat-card">
          <h4>Categorical Columns</h4>
          <p className="stat-value">{categoricalColumns.length}</p>
        </div>
      </div>

      <div className="chart-section">
        <h3>Missing Values Per Column</h3>
        {missingValuesData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={missingValuesData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} interval={0} />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="nullCount" fill="#FF6B6B" name="Null Count" />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <p className="no-data">No missing values data available</p>
        )}
      </div>

      <div className="bg-gray-50 p-6 rounded-xl border border-gray-200 mb-8">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-semibold text-indigo-600">Outlier Detection (IQR Method)</h3>
          <button onClick={() => setOutlierSectionOpen(!outlierSectionOpen)} className="text-indigo-600 hover:text-indigo-800 transition-colors font-medium">
            {outlierSectionOpen ? '▼ Collapse' : '► Expand'}
          </button>
        </div>

        {outlierSectionOpen && (
          <div className="mt-4 space-y-4">
            <div className="bg-white p-4 rounded-lg border border-gray-200">
              <button
                onClick={async () => {
                  if (!onDetectOutliers) {
                    setOutlierError('Outlier detection is not available');
                    return;
                  }

                  setDetectingOutliers(true);
                  setOutlierError('');

                  const result = await onDetectOutliers();

                  if (result.error) {
                    setOutlierError(result.error);
                    setOutlierData(null);
                  } else if (result.data) {
                    const transformedData = {};
                    Object.entries(result.data.outliers).forEach(([column, stats]) => {
                      transformedData[column] = {
                        count: stats.outlier_count,
                        percentage: stats.percentage,
                        total: stats.total_count,
                      };
                    });
                    setOutlierData(transformedData);
                    setOutlierError('');
                  }

                  setDetectingOutliers(false);
                }}
                disabled={detectingOutliers}
                className="bg-gradient-to-r from-indigo-600 to-purple-600 text-white px-6 py-2.5 rounded-lg font-semibold shadow-lg"
              >
                {detectingOutliers ? 'Detecting...' : 'Detect Outliers'}
              </button>

              {outlierError && (
                <div className="mt-4 bg-red-50 border border-red-200 rounded-lg p-4">
                  <p className="text-red-700 text-sm font-medium">{outlierError}</p>
                </div>
              )}
            </div>

            {outlierData && (
              <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column Name</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Outlier Count</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Percentage</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {Object.entries(outlierData).map(([column, stats]) => (
                        <tr key={column} className="hover:bg-gray-50 transition-colors">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{column}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{stats.count}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{stats.percentage.toFixed(1)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {numericColumns.length > 0 && (
        <div className="chart-section">
          <div className="chart-header">
            <h3>Numeric Column Distribution</h3>
            <div className="flex items-center gap-2">
              <select value={selectedNumericColumn} onChange={(e) => setSelectedNumericColumn(e.target.value)} className="column-selector">
                {numericColumns.map((col) => (
                  <option key={col.name} value={col.name}>
                    {col.name}
                  </option>
                ))}
              </select>
              <SemanticBadge semanticType={getSemanticType(selectedNumericColumn)} />
            </div>
          </div>

          {selectedNumericColumn && data.columns[selectedNumericColumn] && (
            <>
              <div className="stats-summary">
                <span>
                  <strong>Min:</strong> {data.columns[selectedNumericColumn].min?.toFixed(2) ?? 'N/A'}
                </span>
                <span>
                  <strong>Max:</strong> {data.columns[selectedNumericColumn].max?.toFixed(2) ?? 'N/A'}
                </span>
                <span>
                  <strong>Mean:</strong> {data.columns[selectedNumericColumn].mean?.toFixed(2) ?? 'N/A'}
                </span>
                <span>
                  <strong>Std:</strong> {data.columns[selectedNumericColumn].std?.toFixed(2) ?? 'N/A'}
                </span>
              </div>

              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={histogramData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="range" angle={-45} textAnchor="end" height={80} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="frequency" fill="#4ECDC4" name="Frequency (approx)" />
                </BarChart>
              </ResponsiveContainer>
            </>
          )}
        </div>
      )}

      {categoricalColumns.length > 0 && (
        <div className="chart-section">
          <div className="chart-header">
            <h3>Categorical Column Distribution</h3>
            <div className="controls">
              <div className="flex items-center gap-2">
                <select value={selectedCategoricalColumn} onChange={(e) => setSelectedCategoricalColumn(e.target.value)} className="column-selector">
                  {categoricalColumns.map((col) => (
                    <option key={col.name} value={col.name}>
                      {col.name}
                    </option>
                  ))}
                </select>
                <SemanticBadge semanticType={getSemanticType(selectedCategoricalColumn)} />
              </div>

              <div className="chart-type-toggle">
                <button className={categoricalChartType === 'pie' ? 'active' : ''} onClick={() => setCategoricalChartType('pie')}>
                  Pie Chart
                </button>
                <button className={categoricalChartType === 'bar' ? 'active' : ''} onClick={() => setCategoricalChartType('bar')}>
                  Bar Chart
                </button>
              </div>
            </div>
          </div>

          {selectedCategoricalColumn && data.columns[selectedCategoricalColumn] && (
            <ResponsiveContainer width="100%" height={300}>
              {categoricalChartType === 'pie' ? (
                <PieChart>
                  <Pie
                    data={categoricalData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {categoricalData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              ) : (
                <BarChart data={categoricalData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="value" name="Count">
                    {categoricalData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              )}
            </ResponsiveContainer>
          )}
        </div>
      )}

      {numericColumns.length > 0 && (
        <div className="chart-section">
          <div className="chart-header">
            <h3>Box Plot Visualization</h3>
            <div className="flex items-center gap-2">
              <select value={selectedBoxPlotColumn} onChange={(e) => setSelectedBoxPlotColumn(e.target.value)} className="column-selector">
                {numericColumns.map((col) => (
                  <option key={col.name} value={col.name}>
                    {col.name}
                  </option>
                ))}
              </select>
              <SemanticBadge semanticType={getSemanticType(selectedBoxPlotColumn)} />
            </div>
          </div>

          {selectedBoxPlotColumn && boxPlotData && (
            <div className="bg-white p-6 rounded-lg">
              <div className="stats-summary mb-4">
                <span>
                  <strong>Min:</strong> {boxPlotData.min.toFixed(2)}
                </span>
                <span>
                  <strong>Q1 (25%):</strong> {boxPlotData.q1.toFixed(2)}
                </span>
                <span>
                  <strong>Median (50%):</strong> {boxPlotData.median.toFixed(2)}
                </span>
                <span>
                  <strong>Q3 (75%):</strong> {boxPlotData.q3.toFixed(2)}
                </span>
                <span>
                  <strong>Max:</strong> {boxPlotData.max.toFixed(2)}
                </span>
                <span>
                  <strong>Outliers:</strong> {boxPlotData.outlierCount}
                </span>
              </div>

              <div className="flex justify-center">
                <BoxPlot data={boxPlotData} columnName={selectedBoxPlotColumn} />
              </div>

              <div className="mt-4 bg-blue-50 border border-blue-200 rounded-lg p-4">
                <p className="text-xs text-blue-700 italic">Quartiles and whiskers are sourced directly from backend profile statistics.</p>
              </div>
            </div>
          )}
        </div>
      )}

      {Object.keys(allBoxPlotData).length > 0 && (
        <section className="mt-6 space-y-4">
          <h2 className="text-xl font-bold text-indigo-600">Box Plot Overview</h2>

          <div className="bg-gradient-to-r from-purple-50 to-indigo-50 p-6 rounded-lg border border-indigo-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">Select Columns for Outlier Removal</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mb-4">
              {Object.keys(allBoxPlotData).map((col) => (
                <label key={col} className="flex items-center justify-between p-2 bg-white rounded-md border border-gray-200">
                  <span className="flex items-center gap-2">
                    <input type="checkbox" checked={!!removeOutliers[col]} onChange={() => toggleOutlierColumn(col)} className="w-4 h-4" />
                    <span className="text-sm font-medium text-gray-700">{col}</span>
                  </span>
                  <SemanticBadge semanticType={getSemanticType(col)} />
                </label>
              ))}
            </div>

            {Object.values(removeOutliers).some(Boolean) && (
              <div className="flex items-center gap-3 pt-4 border-t border-indigo-200">
                <button
                  onClick={handleCleanOutliers}
                  disabled={cleaningOutliers}
                  className="bg-gradient-to-r from-red-600 to-pink-600 text-white px-4 py-2 rounded-lg font-semibold"
                >
                  {cleaningOutliers
                    ? 'Cleaning...'
                    : `Remove Outliers from Selected Columns (${Object.values(removeOutliers).filter(Boolean).length})`}
                </button>
                <button onClick={() => setRemoveOutliers({})} className="text-gray-600 hover:text-gray-800 font-medium text-sm underline">
                  Clear Selection
                </button>
              </div>
            )}

            {cleaningSuccess && (
              <div className="mt-4 bg-green-50 border border-green-200 rounded-lg p-4">
                <p className="text-green-700 font-medium">Outliers successfully removed</p>
                <p className="text-green-600 text-sm mt-1">
                  Removed {cleaningSuccess.rowsRemoved} rows ({cleaningSuccess.rowsBefore} → {cleaningSuccess.rowsAfter} rows)
                </p>
              </div>
            )}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {Object.entries(allBoxPlotData).map(([col, stats]) => (
              <div key={col} className="relative">
                <BoxPlotCard column={col} stats={stats} semanticType={getSemanticType(col)} />
                {removeOutliers[col] && (
                  <div className="absolute top-2 right-2">
                    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold bg-red-100 text-red-800">Selected</span>
                  </div>
                )}
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
};

export default DataVisualizations;
