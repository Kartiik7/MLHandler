import React, { useState, useEffect } from 'react';
import { API_BASE } from '../config';

export default function DatasetComparison({ defaultTaskId }) {
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(true);
  const [taskIdA, setTaskIdA] = useState('');
  const [taskIdB, setTaskIdB] = useState('');
  const [compareData, setCompareData] = useState(null);
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [error, setError] = useState(null);

  // Fetch run history
  useEffect(() => {
    let active = true;
    async function fetchHistory() {
      try {
        const res = await fetch(`${API_BASE}/datasets`);
        if (!res.ok) throw new Error('Failed to fetch dataset history');
        const data = await res.json();
        if (active) {
          setHistory(data);
          // Auto-select defaults
          if (data.length > 0) {
            const defaultA = defaultTaskId && data.some(d => d.task_id === defaultTaskId)
              ? defaultTaskId
              : data[0].task_id;
            setTaskIdA(defaultA);
            
            if (data.length > 1) {
              // Select the second most recent run for B if available
              const firstOther = data.find(d => d.task_id !== defaultA);
              if (firstOther) {
                setTaskIdB(firstOther.task_id);
              }
            } else {
              setTaskIdB(data[0].task_id);
            }
          }
        }
      } catch (err) {
        console.error(err);
      } finally {
        if (active) {
          setLoadingHistory(false);
        }
      }
    }
    fetchHistory();
    return () => {
      active = false;
    };
  }, [defaultTaskId]);

  // Run comparison
  const handleCompare = async () => {
    if (!taskIdA || !taskIdB) {
      setError('Please select both Dataset A and Dataset B to compare.');
      return;
    }
    setLoadingCompare(true);
    setError(null);
    setCompareData(null);

    try {
      const res = await fetch(`${API_BASE}/compare/${taskIdA}/${taskIdB}`);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Failed to compare datasets (${res.status})`);
      }
      const data = await res.json();
      setCompareData(data);
    } catch (err) {
      setError(err.message || 'Error occurred during comparison.');
    } finally {
      setLoadingCompare(false);
    }
  };

  if (loadingHistory) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-500 gap-3">
        <svg className="animate-spin h-5 w-5 text-indigo-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
        </svg>
        <span className="text-sm">Loading dataset history...</span>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-6xl mx-auto py-4">
      {/* Selection Panel */}
      <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm">
        <h3 className="text-lg font-bold text-gray-900 mb-4 flex items-center gap-2">
          <svg className="h-5 w-5 text-indigo-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
          </svg>
          Dataset Comparison Studio
        </h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-end">
          <div className="space-y-1">
            <label className="text-xs font-bold text-gray-500 uppercase">Base Dataset (A)</label>
            <select
              value={taskIdA}
              onChange={(e) => setTaskIdA(e.target.value)}
              className="w-full bg-gray-50 border border-gray-200 rounded-lg p-2.5 text-sm font-medium focus:ring-2 focus:ring-indigo-500 focus:outline-none"
            >
              <option value="">Select Base Dataset...</option>
              {history.map(d => (
                <option key={d.task_id} value={d.task_id}>
                  {d.filename} ({d.rows.toLocaleString()} rows × {d.columns} cols) — {new Date(d.processed_at).toLocaleTimeString()}
                </option>
              ))}
            </select>
          </div>

          <div className="space-y-1">
            <label className="text-xs font-bold text-gray-500 uppercase">Target Dataset (B)</label>
            <select
              value={taskIdB}
              onChange={(e) => setTaskIdB(e.target.value)}
              className="w-full bg-gray-50 border border-gray-200 rounded-lg p-2.5 text-sm font-medium focus:ring-2 focus:ring-indigo-500 focus:outline-none"
            >
              <option value="">Select Target Dataset...</option>
              {history.map(d => (
                <option key={d.task_id} value={d.task_id}>
                  {d.filename} ({d.rows.toLocaleString()} rows × {d.columns} cols) — {new Date(d.processed_at).toLocaleTimeString()}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="mt-4 flex items-center justify-between">
          <button
            type="button"
            onClick={handleCompare}
            disabled={!taskIdA || !taskIdB || loadingCompare}
            className="submit-btn px-6 py-2.5 rounded-lg text-sm font-semibold text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 transition-colors"
          >
            {loadingCompare ? 'Analyzing deltas...' : 'Run Comparison'}
          </button>
          
          {history.length < 2 && (
            <span className="text-xs text-amber-600 font-semibold bg-amber-50 px-3 py-1 rounded-md border border-amber-200">
              💡 Process more files to unlock side-by-side comparison metrics.
            </span>
          )}
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm max-w-2xl mx-auto">
          {error}
        </div>
      )}

      {loadingCompare && (
        <div className="flex items-center justify-center h-48 text-gray-500 gap-3">
          <svg className="animate-spin h-6 w-6 text-indigo-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
          </svg>
          <span className="text-sm font-medium">Computing structural and quality differences...</span>
        </div>
      )}

      {/* Comparison Report Output */}
      {compareData && (
        <div className="space-y-6">
          
          {/* Quality Scores Delta Panel */}
          <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm">
            <h4 className="text-sm font-bold text-gray-900 uppercase tracking-wider mb-4">Quality Score Comparison</h4>
            
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
              {Object.entries(compareData.quality_diff).map(([key, diffObj]) => {
                const isPositive = diffObj.diff > 0;
                const isZero = diffObj.diff === 0;
                const displayDiff = isZero ? '0' : (isPositive ? `+${diffObj.diff}` : diffObj.diff);
                const badgeColor = isZero ? 'text-gray-500 bg-gray-100' : (isPositive ? 'text-teal-700 bg-teal-50 border-teal-200' : 'text-red-700 bg-red-50 border-red-200');
                
                return (
                  <div key={key} className="bg-gray-50 border border-gray-100 rounded-xl p-4 flex flex-col justify-between text-center relative overflow-hidden group">
                    <span className="text-xs font-bold text-gray-400 capitalize tracking-tight mb-2">
                      {key.replace('_score', '')}
                    </span>
                    <div className="flex items-baseline justify-center gap-1.5">
                      <span className="text-2xl font-extrabold text-gray-900">{diffObj.b}</span>
                      <span className="text-xs text-gray-400">vs {diffObj.a}</span>
                    </div>
                    <div className={`mt-3 self-center text-xs font-bold px-2 py-0.5 rounded-full border ${badgeColor}`}>
                      {displayDiff}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Dataset Dimensions Delta */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Rows Delta */}
            <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm flex items-center justify-between">
              <div>
                <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Row Count Transition</span>
                <h4 className="text-2xl font-extrabold text-gray-900 mt-1">
                  {compareData.row_diff.b.toLocaleString()}
                  <span className="text-sm font-medium text-gray-400 ml-2">from {compareData.row_diff.a.toLocaleString()}</span>
                </h4>
              </div>
              <div className={`text-right px-4 py-2 rounded-xl font-bold text-sm border ${
                compareData.row_diff.difference === 0
                  ? 'text-gray-500 bg-gray-50 border-gray-200'
                  : compareData.row_diff.difference > 0
                    ? 'text-teal-700 bg-teal-50 border-teal-200'
                    : 'text-red-700 bg-red-50 border-red-200'
              }`}>
                {compareData.row_diff.difference === 0 ? 'No change' : (
                  <span>
                    {compareData.row_diff.difference > 0 ? '+' : ''}
                    {compareData.row_diff.difference.toLocaleString()} ({compareData.row_diff.percent_change.toFixed(2)}%)
                  </span>
                )}
              </div>
            </div>

            {/* Columns Delta */}
            <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm flex items-center justify-between">
              <div>
                <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Columns Count Transition</span>
                <h4 className="text-2xl font-extrabold text-gray-900 mt-1">
                  {compareData.col_diff.b}
                  <span className="text-sm font-medium text-gray-400 ml-2">from {compareData.col_diff.a}</span>
                </h4>
              </div>
              <div className={`text-right px-4 py-2 rounded-xl font-bold text-sm border ${
                compareData.col_diff.difference === 0
                  ? 'text-gray-500 bg-gray-50 border-gray-200'
                  : compareData.col_diff.difference > 0
                    ? 'text-teal-700 bg-teal-50 border-teal-200'
                    : 'text-red-700 bg-red-50 border-red-200'
              }`}>
                {compareData.col_diff.difference === 0 ? 'No change' : (
                  <span>
                    {compareData.col_diff.difference > 0 ? '+' : ''}
                    {compareData.col_diff.difference}
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Schema Structural Changes */}
          <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm space-y-6">
            <h4 className="text-sm font-bold text-gray-900 uppercase tracking-wider">Schema Structural Deltas</h4>
            
            {/* Added Columns */}
            <div>
              <span className="text-xs font-bold text-teal-600 block mb-2">Added Columns ({compareData.columns_added.length})</span>
              {compareData.columns_added.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {compareData.columns_added.map(c => (
                    <span key={c} className="bg-teal-50 text-teal-700 border border-teal-200 text-xs px-2.5 py-1 rounded-md font-mono">
                      +{c}
                    </span>
                  ))}
                </div>
              ) : (
                <span className="text-xs text-gray-400 italic">No columns added</span>
              )}
            </div>

            {/* Removed Columns */}
            <div className="pt-2 border-t border-gray-100">
              <span className="text-xs font-bold text-red-600 block mb-2">Removed Columns ({compareData.columns_removed.length})</span>
              {compareData.columns_removed.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {compareData.columns_removed.map(c => (
                    <span key={c} className="bg-red-50 text-red-700 border border-red-200 text-xs px-2.5 py-1 rounded-md font-mono">
                      -{c}
                    </span>
                  ))}
                </div>
              ) : (
                <span className="text-xs text-gray-400 italic">No columns removed</span>
              )}
            </div>

            {/* Type Changes */}
            {Object.keys(compareData.type_changes).length > 0 && (
              <div className="pt-4 border-t border-gray-100">
                <span className="text-xs font-bold text-amber-600 block mb-2">Column Type Changes ({Object.keys(compareData.type_changes).length})</span>
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
                  {Object.entries(compareData.type_changes).map(([col, types]) => (
                    <div key={col} className="bg-amber-50 border border-amber-100 rounded-lg p-3 text-xs text-amber-900 flex flex-col gap-1 font-mono">
                      <span className="font-bold border-b border-amber-200 pb-1 mb-1">{col}</span>
                      <div className="flex justify-between items-center text-xs">
                        <span className="bg-white px-1.5 py-0.5 rounded border border-amber-200">{types.a}</span>
                        <span className="text-amber-500 font-bold">→</span>
                        <span className="bg-amber-600 text-white px-1.5 py-0.5 rounded">{types.b}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Missing Value Differences Table */}
          {Object.keys(compareData.missing_diff).length > 0 && (
            <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
              <div className="p-5 border-b border-gray-100">
                <h4 className="text-sm font-bold text-gray-900 uppercase tracking-wider">Missing Value Deltas</h4>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-gray-50 text-gray-500 text-xs uppercase font-bold border-b border-gray-100">
                    <tr>
                      <th className="px-6 py-3">Column</th>
                      <th className="px-6 py-3">Dataset A Missing</th>
                      <th className="px-6 py-3">Dataset B Missing</th>
                      <th className="px-6 py-3">Delta %</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 text-gray-700">
                    {Object.entries(compareData.missing_diff).map(([col, diff]) => {
                      const isNegative = diff.diff_percent < 0;
                      const isZero = diff.diff_percent === 0;
                      const textClass = isZero ? 'text-gray-500' : (isNegative ? 'text-teal-600 font-semibold' : 'text-red-600 font-semibold');
                      const displayDiff = isZero ? '0.00%' : `${diff.diff_percent > 0 ? '+' : ''}${(diff.diff_percent * 100).toFixed(2)}%`;
                      
                      return (
                        <tr key={col} className="hover:bg-gray-50/50">
                          <td className="px-6 py-4 font-mono font-semibold text-gray-900">{col}</td>
                          <td className="px-6 py-4">
                            {diff.a_count.toLocaleString()} ({(diff.a_percent * 100).toFixed(2)}%)
                          </td>
                          <td className="px-6 py-4">
                            {diff.b_count.toLocaleString()} ({(diff.b_percent * 100).toFixed(2)}%)
                          </td>
                          <td className={`px-6 py-4 ${textClass}`}>
                            {displayDiff}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Numeric Distribution Shifts */}
          {compareData.distribution_diff && Object.keys(compareData.distribution_diff).length > 0 && (
            <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
              <div className="p-5 border-b border-gray-100">
                <h4 className="text-sm font-bold text-gray-900 uppercase tracking-wider">Numeric Distribution Shifts</h4>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-gray-50 text-gray-500 text-xs uppercase font-bold border-b border-gray-100">
                    <tr>
                      <th className="px-6 py-3">Column</th>
                      <th className="px-6 py-3">Metric</th>
                      <th className="px-6 py-3">Dataset A</th>
                      <th className="px-6 py-3">Dataset B</th>
                      <th className="px-6 py-3">Delta</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 text-gray-700">
                    {Object.entries(compareData.distribution_diff).map(([col, metrics]) => {
                      return (
                        <React.Fragment key={col}>
                          {/* Mean Row */}
                          <tr className="hover:bg-gray-50/50">
                            <td className="px-6 py-4 font-mono font-semibold text-gray-900" rowSpan={3}>
                              {col}
                            </td>
                            <td className="px-6 py-3 text-xs text-gray-500 font-semibold uppercase">Mean</td>
                            <td className="px-6 py-3">{metrics.mean.a !== null ? metrics.mean.a.toFixed(4) : 'N/A'}</td>
                            <td className="px-6 py-3">{metrics.mean.b !== null ? metrics.mean.b.toFixed(4) : 'N/A'}</td>
                            <td className={`px-6 py-3 font-semibold ${
                              metrics.mean.diff === null || metrics.mean.diff === 0
                                ? 'text-gray-500'
                                : metrics.mean.diff > 0
                                  ? 'text-teal-600'
                                  : 'text-red-600'
                            }`}>
                              {metrics.mean.diff !== null ? (metrics.mean.diff > 0 ? `+${metrics.mean.diff}` : metrics.mean.diff) : 'N/A'}
                            </td>
                          </tr>
                          {/* Median Row */}
                          <tr className="hover:bg-gray-50/50">
                            <td className="px-6 py-3 text-xs text-gray-500 font-semibold uppercase">Median</td>
                            <td className="px-6 py-3">{metrics.median.a !== null ? metrics.median.a.toFixed(4) : 'N/A'}</td>
                            <td className="px-6 py-3">{metrics.median.b !== null ? metrics.median.b.toFixed(4) : 'N/A'}</td>
                            <td className={`px-6 py-3 font-semibold ${
                              metrics.median.diff === null || metrics.median.diff === 0
                                ? 'text-gray-500'
                                : metrics.median.diff > 0
                                  ? 'text-teal-600'
                                  : 'text-red-600'
                            }`}>
                              {metrics.median.diff !== null ? (metrics.median.diff > 0 ? `+${metrics.median.diff}` : metrics.median.diff) : 'N/A'}
                            </td>
                          </tr>
                          {/* Std Dev Row */}
                          <tr className="hover:bg-gray-50/50 border-b border-gray-100">
                            <td className="px-6 py-3 text-xs text-gray-500 font-semibold uppercase">Std Dev</td>
                            <td className="px-6 py-3">{metrics.std.a !== null ? metrics.std.a.toFixed(4) : 'N/A'}</td>
                            <td className="px-6 py-3">{metrics.std.b !== null ? metrics.std.b.toFixed(4) : 'N/A'}</td>
                            <td className={`px-6 py-3 font-semibold ${
                              metrics.std.diff === null || metrics.std.diff === 0
                                ? 'text-gray-500'
                                : metrics.std.diff > 0
                                  ? 'text-teal-600'
                                  : 'text-red-600'
                            }`}>
                              {metrics.std.diff !== null ? (metrics.std.diff > 0 ? `+${metrics.std.diff}` : metrics.std.diff) : 'N/A'}
                            </td>
                          </tr>
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Correlation Shifts */}
          {compareData.correlation_diff && compareData.correlation_diff.length > 0 && (
            <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
              <div className="p-5 border-b border-gray-100">
                <h4 className="text-sm font-bold text-gray-900 uppercase tracking-wider">Correlation Shifts (Data Drift)</h4>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-gray-50 text-gray-500 text-xs uppercase font-bold border-b border-gray-100">
                    <tr>
                      <th className="px-6 py-3">Feature Pair</th>
                      <th className="px-6 py-3 text-center">Dataset A (r)</th>
                      <th className="px-6 py-3 text-center">Dataset B (r)</th>
                      <th className="px-6 py-3 text-center">Shift delta</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 text-gray-700">
                    {compareData.correlation_diff.slice(0, 15).map((shift, idx) => {
                      const isPositive = shift.diff > 0;
                      const isZero = shift.diff === 0;
                      const shiftVal = isZero ? '0' : (isPositive ? `+${shift.diff}` : shift.diff);
                      const textClass = isZero ? 'text-gray-500' : (isPositive ? 'text-teal-600 font-semibold' : 'text-red-600 font-semibold');
                      
                      return (
                        <tr key={idx} className="hover:bg-gray-50/50">
                          <td className="px-6 py-4 font-mono font-semibold text-gray-900">
                            {shift.col1} <span className="text-gray-400 font-normal">×</span> {shift.col2}
                          </td>
                          <td className="px-6 py-4 text-center font-semibold text-gray-600">{shift.r_a.toFixed(3)}</td>
                          <td className="px-6 py-4 text-center font-semibold text-gray-900">{shift.r_b.toFixed(3)}</td>
                          <td className={`px-6 py-4 text-center ${textClass}`}>
                            {shiftVal}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
