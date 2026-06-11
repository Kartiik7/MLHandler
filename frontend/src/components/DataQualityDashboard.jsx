import React, { useState, useEffect } from 'react';
import { API_BASE } from '../config';

function getScoreColor(score) {
  if (score >= 80) return 'text-teal-600 bg-teal-50 border-teal-200 stroke-teal-500';
  if (score >= 60) return 'text-amber-600 bg-amber-50 border-amber-200 stroke-amber-500';
  return 'text-red-600 bg-red-50 border-red-200 stroke-red-500';
}

function getScoreBgColor(score) {
  if (score >= 80) return 'bg-teal-500';
  if (score >= 60) return 'bg-amber-500';
  return 'bg-red-500';
}

function getGrade(score) {
  if (score >= 90) return 'A';
  if (score >= 80) return 'B';
  if (score >= 70) return 'C';
  if (score >= 60) return 'D';
  return 'F';
}

function getGradeDescription(grade) {
  switch (grade) {
    case 'A': return 'Excellent dataset quality with minimal or no significant anomalies.';
    case 'B': return 'Good dataset quality. Some minor issues detected but ready for ML.';
    case 'C': return 'Fair dataset quality. Requires moderate cleaning or review.';
    case 'D': return 'Poor quality. Heavy anomalies detected. ML performance may degrade.';
    default: return 'Critical quality issues. Do not use without manual data audit.';
  }
}

export default function DataQualityDashboard({ taskId }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedDetails, setExpandedDetails] = useState(false);

  useEffect(() => {
    if (!taskId) return;
    setLoading(true);
    setError(null);
    let active = true;

    async function fetchQualityScore() {
      try {
        const res = await fetch(`${API_BASE}/quality-score/${taskId}`);
        if (!res.ok) {
          const errBody = await res.json().catch(() => ({}));
          throw new Error(errBody.error || `Server responded with status ${res.status}`);
        }
        const payload = await res.json();
        if (active) {
          setData(payload);
        }
      } catch (err) {
        if (active) {
          setError(err.message || 'Failed to fetch data quality report.');
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    fetchQualityScore();
    return () => {
      active = false;
    };
  }, [taskId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 gap-3">
        <svg className="animate-spin h-6 w-6 text-indigo-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
        </svg>
        <span className="text-sm font-medium">Calculating data quality scores...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm max-w-2xl mx-auto my-6">
        <p className="font-semibold mb-1">Failed to Load Quality Score</p>
        <p>{error}</p>
      </div>
    );
  }

  if (!data) return null;

  const { quality_score, missing_score, duplicate_score, outlier_score, type_score, empty_score, details } = data;
  const grade = getGrade(quality_score);
  const strokeColorClass = getScoreColor(quality_score).split(' ').find(c => c.startsWith('stroke-')) || 'stroke-teal-500';
  const textColorClass = getScoreColor(quality_score).split(' ').find(c => c.startsWith('text-')) || 'text-teal-600';
  const bgColorClass = getScoreColor(quality_score).split(' ').find(c => c.startsWith('bg-')) || 'bg-teal-50';
  const borderClass = getScoreColor(quality_score).split(' ').find(c => c.startsWith('border-')) || 'border-teal-200';

  // SVG Gauge calculations
  const radius = 60;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - (quality_score / 100) * circumference;

  return (
    <div className="space-y-6 max-w-6xl mx-auto py-4">
      {/* Overview Hero Card */}
      <div className={`flex flex-col md:flex-row items-center gap-6 p-6 rounded-2xl border ${bgColorClass} ${borderClass} shadow-sm transition-all`}>
        {/* Animated Circular Gauge */}
        <div className="relative flex-shrink-0 w-36 h-36 flex items-center justify-center">
          <svg className="w-full h-full transform -rotate-90">
            <circle cx="72" cy="72" r={radius} className="stroke-gray-200 fill-none" strokeWidth="10" />
            <circle
              cx="72"
              cy="72"
              r={radius}
              className={`fill-none transition-all duration-1000 ease-out ${strokeColorClass}`}
              strokeWidth="10"
              strokeDasharray={circumference}
              strokeDashoffset={strokeDashoffset}
              strokeLinecap="round"
            />
          </svg>
          <div className="absolute flex flex-col items-center justify-center text-center">
            <span className="text-4xl font-extrabold tracking-tight text-gray-900">{quality_score}</span>
            <span className="text-xs font-semibold text-gray-500 uppercase">Score</span>
          </div>
        </div>

        {/* Overview explanation */}
        <div className="flex-1 text-center md:text-left space-y-2">
          <div className="flex items-center justify-center md:justify-start gap-3">
            <span className={`text-sm font-bold px-3 py-1 rounded-full uppercase tracking-wider ${textColorClass} bg-white border border-current`}>
              Grade {grade}
            </span>
            <h2 className="text-xl font-bold text-gray-900">Data Quality Score</h2>
          </div>
          <p className="text-sm text-gray-600 leading-relaxed max-w-2xl">
            {getGradeDescription(grade)}
          </p>
          <div className="flex flex-wrap items-center justify-center md:justify-start gap-x-6 gap-y-2 pt-2 text-xs text-gray-500">
            <div>
              <strong>Original Dimensions:</strong> {details.duplicates.total_rows} rows × {details.empty_columns.total_columns} columns
            </div>
            <div className="hidden md:block text-gray-300">|</div>
            <div>
              <strong>Cleaned Dimensions:</strong> {details.duplicates.total_rows - details.duplicates.duplicates_removed} rows × {details.empty_columns.total_columns - details.empty_columns.dropped_columns.length} columns
            </div>
          </div>
        </div>
      </div>

      {/* Sub-Score Cards Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        {/* Missing Values Card */}
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 group">
          <div className="flex justify-between items-start mb-2">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Missing Values</span>
            <span className={`text-lg font-bold ${getScoreColor(missing_score).split(' ')[0]}`}>{missing_score}</span>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden mb-3">
            <div className={`h-full rounded-full transition-all duration-1000 ${getScoreBgColor(missing_score)}`} style={{ width: `${missing_score}%` }} />
          </div>
          <p className="text-xs text-gray-500">
            {details.missing.missing_cells > 0 ? (
              <span><strong>{(details.missing.missing_ratio * 100).toFixed(2)}%</strong> missing ({details.missing.missing_cells.toLocaleString()} cells)</span>
            ) : (
              <span>No missing values detected</span>
            )}
          </p>
        </div>

        {/* Duplicate Rows Card */}
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 group">
          <div className="flex justify-between items-start mb-2">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Duplicates</span>
            <span className={`text-lg font-bold ${getScoreColor(duplicate_score).split(' ')[0]}`}>{duplicate_score}</span>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden mb-3">
            <div className={`h-full rounded-full transition-all duration-1000 ${getScoreBgColor(duplicate_score)}`} style={{ width: `${duplicate_score}%` }} />
          </div>
          <p className="text-xs text-gray-500">
            {details.duplicates.duplicates_removed > 0 ? (
              <span><strong>{details.duplicates.duplicates_removed.toLocaleString()}</strong> duplicate rows removed</span>
            ) : (
              <span>No duplicate rows detected</span>
            )}
          </p>
        </div>

        {/* Outliers Card */}
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 group">
          <div className="flex justify-between items-start mb-2">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Outliers</span>
            <span className={`text-lg font-bold ${getScoreColor(outlier_score).split(' ')[0]}`}>{outlier_score}</span>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden mb-3">
            <div className={`h-full rounded-full transition-all duration-1000 ${getScoreBgColor(outlier_score)}`} style={{ width: `${outlier_score}%` }} />
          </div>
          <p className="text-xs text-gray-500">
            {details.outliers.outlier_cells > 0 ? (
              <span><strong>{(details.outliers.outlier_ratio * 100).toFixed(2)}%</strong> outliers ({details.outliers.outlier_cells.toLocaleString()} cells)</span>
            ) : (
              <span>No outliers detected</span>
            )}
          </p>
        </div>

        {/* Invalid Data Types Card */}
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 group">
          <div className="flex justify-between items-start mb-2">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Invalid Types</span>
            <span className={`text-lg font-bold ${getScoreColor(type_score).split(' ')[0]}`}>{type_score}</span>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden mb-3">
            <div className={`h-full rounded-full transition-all duration-1000 ${getScoreBgColor(type_score)}`} style={{ width: `${type_score}%` }} />
          </div>
          <p className="text-xs text-gray-500">
            {details.types.mixed_type_columns.length > 0 || details.types.schema_type_issues > 0 || Object.keys(details.types.coercion_failures).length > 0 ? (
              <span>Issues: {details.types.mixed_type_columns.length + details.types.schema_type_issues + Object.keys(details.types.coercion_failures).length} columns</span>
            ) : (
              <span>Types fully consistent</span>
            )}
          </p>
        </div>

        {/* Empty Columns Card */}
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-200 group">
          <div className="flex justify-between items-start mb-2">
            <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Empty Columns</span>
            <span className={`text-lg font-bold ${getScoreColor(empty_score).split(' ')[0]}`}>{empty_score}</span>
          </div>
          <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden mb-3">
            <div className={`h-full rounded-full transition-all duration-1000 ${getScoreBgColor(empty_score)}`} style={{ width: `${empty_score}%` }} />
          </div>
          <p className="text-xs text-gray-500">
            {details.empty_columns.dropped_columns.length > 0 ? (
              <span><strong>{details.empty_columns.dropped_columns.length}</strong> empty columns dropped</span>
            ) : (
              <span>No empty columns found</span>
            )}
          </p>
        </div>
      </div>

      {/* Deep Dive Details Section */}
      <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
        <button
          type="button"
          onClick={() => setExpandedDetails(!expandedDetails)}
          className="w-full flex items-center justify-between p-5 text-left font-bold text-gray-900 border-b border-gray-100 hover:bg-gray-50 transition-colors"
        >
          <div className="flex items-center gap-2">
            <svg className="h-5 w-5 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <span>Detailed Quality Deep-Dive</span>
          </div>
          <svg
            className={`h-5 w-5 text-gray-500 transform transition-transform ${expandedDetails ? 'rotate-180' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {expandedDetails && (
          <div className="p-5 space-y-6 divide-y divide-gray-100">
            {/* Empty Columns Section */}
            {details.empty_columns.dropped_columns.length > 0 && (
              <div className="pb-5">
                <h4 className="text-sm font-semibold text-red-600 mb-2">Completely Empty Columns Dropped</h4>
                <div className="flex flex-wrap gap-2">
                  {details.empty_columns.dropped_columns.map(col => (
                    <span key={col} className="bg-red-50 text-red-700 border border-red-200 text-xs px-2.5 py-1 rounded-md font-mono">
                      {col}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Mixed Types Section */}
            {details.types.mixed_type_columns.length > 0 && (
              <div className="py-5">
                <h4 className="text-sm font-semibold text-amber-600 mb-2">Mixed Type Warnings</h4>
                <p className="text-xs text-gray-500 mb-3">
                  These columns contain multiple data types within the same field, which represents a data ingestion anomaly.
                </p>
                <div className="flex flex-wrap gap-2">
                  {details.types.mixed_type_columns.map(col => (
                    <span key={col} className="bg-amber-50 text-amber-700 border border-amber-200 text-xs px-2.5 py-1 rounded-md font-mono">
                      {col} (Multiple types)
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Coercion Failures Section */}
            {Object.keys(details.types.coercion_failures).length > 0 && (
              <div className="py-5">
                <h4 className="text-sm font-semibold text-indigo-600 mb-2">Coercion Failure Anomalies</h4>
                <p className="text-xs text-gray-500 mb-3">
                  These columns underwent automatic numeric or date coercion, and some values failed to parse, generating NaNs that had to be imputed.
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                  {Object.entries(details.types.coercion_failures).map(([col, count]) => (
                    <div key={col} className="bg-indigo-50 border border-indigo-100 rounded-lg p-3 text-xs text-indigo-900 flex justify-between items-center font-mono">
                      <span>{col}</span>
                      <span className="font-semibold font-sans">{count} failed values ({((count / details.duplicates.total_rows) * 100).toFixed(1)}%)</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Clean summary info */}
            <div className="py-5">
              <h4 className="text-sm font-semibold text-gray-800 mb-3">Dataset Health Summary</h4>
              <ul className="text-xs text-gray-600 space-y-2 list-disc list-inside">
                <li>Total cells: <span className="font-semibold text-gray-900">{details.missing.total_cells.toLocaleString()}</span></li>
                <li>Missing cell rate: <span className="font-semibold text-gray-900">{(details.missing.missing_ratio * 100).toFixed(3)}%</span></li>
                <li>Duplicate rows removed: <span className="font-semibold text-gray-900">{details.duplicates.duplicates_removed.toLocaleString()} ({((details.duplicates.duplicates_removed / details.duplicates.total_rows) * 100).toFixed(2)}%)</span></li>
                <li>Outlier cells found: <span className="font-semibold text-gray-900">{details.outliers.outlier_cells.toLocaleString()} ({((details.outliers.outlier_ratio) * 100).toFixed(2)}% of numeric cells)</span></li>
              </ul>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
