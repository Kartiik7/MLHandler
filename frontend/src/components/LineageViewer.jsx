import React, { useState } from 'react';

export default function LineageViewer({ lineage, report }) {
  const [selectedStage, setSelectedStage] = useState(null);

  if (!lineage || !Array.isArray(lineage.events)) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-gray-600">
        No lineage data available for this task.
      </div>
    );
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Extract Stats & Events
  // ─────────────────────────────────────────────────────────────────────────────
  const events = lineage.events;
  const rowsBefore = report?.rows_before ?? 0;
  const rowsAfter = report?.rows_after ?? 0;
  const duplicates = report?.duplicates_removed ?? 0;
  const columnsBefore = report?.columns_before ?? 0;
  const columnsAfter = report?.columns_after ?? 0;
  
  // Find specific events
  const loadEvent = events.find(e => e.action === 'file_loaded');
  const typeEvent = events.find(e => e.action === 'types_converted');
  const cleanEvent = events.find(e => e.action === 'nulls_imputed');
  const outlierEvent = events.find(e => e.action === 'outliers_removed');
  const validateEvent = events.find(e => e.action === 'columns_renamed');

  const outliersCount = outlierEvent?.affected_count ?? 0;
  const outliersEnabled = report?.config_used?.remove_outliers ?? false;

  // Calculate row counts after each stage
  const countAfterLoad = rowsBefore;
  const countAfterMapping = rowsBefore;
  const countAfterTypeFix = rowsBefore;
  const countAfterClean = rowsBefore - duplicates;
  const countAfterOutliers = rowsAfter;
  const countAfterValidate = rowsAfter;
  const countAfterProfile = rowsAfter;

  // ─────────────────────────────────────────────────────────────────────────────
  // Define Pipeline Stages
  // ─────────────────────────────────────────────────────────────────────────────
  const stages = [
    {
      id: 'load',
      name: 'Load CSV',
      description: 'Ingest raw CSV file, parse encoding, and load initial dimensions.',
      status: loadEvent ? 'success' : 'skipped',
      metrics: {
        'Inferred Encoding': report?.load_metadata?.encoding || 'utf-8',
        'Raw Rows': rowsBefore.toLocaleString(),
        'Raw Columns': columnsBefore
      },
      rawEvent: loadEvent,
      rowCount: countAfterLoad
    },
    {
      id: 'mapping',
      name: 'Field Mapping',
      description: 'Apply schema mapping rules to rename and align raw columns to targets.',
      status: report?.field_mapping?.mapped_count > 0 ? 'success' : 'skipped',
      metrics: {
        'Mapped Fields': report?.field_mapping?.mapped_count ?? 0,
        'Unmapped Fields': report?.field_mapping?.unmapped_count ?? 0
      },
      rawEvent: null,
      rowCount: countAfterMapping
    },
    {
      id: 'types',
      name: 'Type Fixing',
      description: 'Coerce columns to numeric/datetime based on schema patterns or heuristics.',
      status: typeEvent && typeEvent.affected_count > 0 ? 'success' : 'skipped',
      metrics: {
        'Types Changed': typeEvent?.affected_count ?? 0,
        'Coercions Attempted': Object.keys(report?.type_conversions ?? {}).length
      },
      rawEvent: typeEvent,
      rowCount: countAfterTypeFix
    },
    {
      id: 'cleaning',
      name: 'Cleaning & Dups',
      description: 'Normalize missing tokens, remove duplicates, and impute null cells.',
      status: cleanEvent ? 'success' : 'skipped',
      metrics: {
        'Duplicates Removed': duplicates.toLocaleString(),
        'Missing Values Filled': report?.missing_filled_total?.toLocaleString() ?? 0
      },
      rawEvent: cleanEvent,
      rowCount: countAfterClean
    },
    {
      id: 'outliers',
      name: 'Outlier Removal',
      description: 'Identify and remove numeric outliers using IQR threshold (1.5x).',
      status: outliersEnabled ? (outliersCount > 0 ? 'success' : 'skipped') : 'skipped',
      metrics: {
        'Outliers Removed': outliersCount.toLocaleString(),
        'Columns Affected': outlierEvent?.details?.columns_affected?.length ?? 0
      },
      rawEvent: outlierEvent,
      rowCount: countAfterOutliers
    },
    {
      id: 'validation',
      name: 'Validation',
      description: 'Verify dataset integrity against schema rules and rename duplicate columns.',
      status: report?.schema_validation?.length > 0 ? 'warning' : 'success',
      metrics: {
        'Schema Issues Found': report?.schema_validation?.length ?? 0,
        'Columns Auto-renamed': Object.keys(report?.columns_renamed ?? {}).length
      },
      rawEvent: validateEvent,
      rowCount: countAfterValidate
    },
    {
      id: 'profiling',
      name: 'Profiling',
      description: 'Compute statistical aggregates, missing ratios, and numeric distributions.',
      status: report?.profile ? 'success' : 'skipped',
      metrics: {
        'Final Rows': rowsAfter.toLocaleString(),
        'Final Columns': columnsAfter
      },
      rawEvent: null,
      rowCount: countAfterProfile
    },
    {
      id: 'export',
      name: 'Export Output',
      description: 'Generate Parquet, Excel, and CSV artifacts for downstream use.',
      status: report ? 'success' : 'skipped',
      metrics: {
        'Export Status': 'Complete',
        'Formats Produced': 'CSV, Parquet, Excel'
      },
      rawEvent: null,
      rowCount: countAfterProfile
    }
  ];

  const currentStageInfo = selectedStage ? stages.find(s => s.id === selectedStage) : null;

  return (
    <div className="space-y-8 max-w-6xl mx-auto py-4">
      {/* Introduction text */}
      <div className="flex justify-between items-center">
        <div className="text-sm text-gray-500">
          Click on any node in the pipeline flowchart to inspect step details and raw metadata.
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-teal-500" /> Success</span>
          <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-amber-500" /> Warning</span>
          <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-gray-300" /> Skipped</span>
        </div>
      </div>

      {/* Responsive Flowchart Diagram */}
      <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm overflow-x-auto">
        <div className="flex flex-col md:flex-row items-center md:justify-between min-w-max gap-y-6 md:gap-y-0 px-2 py-4">
          {stages.map((stage, idx) => {
            const isSelected = selectedStage === stage.id;
            
            // Set colors based on status and selection
            let statusColor = 'border-gray-200 bg-gray-50 text-gray-500 hover:bg-gray-100/70';
            if (stage.status === 'success') {
              statusColor = isSelected 
                ? 'border-teal-500 bg-teal-50 text-teal-800 ring-2 ring-teal-200 shadow-md' 
                : 'border-teal-200 bg-teal-50/50 text-teal-700 hover:bg-teal-50/90 hover:border-teal-300';
            } else if (stage.status === 'warning') {
              statusColor = isSelected 
                ? 'border-amber-500 bg-amber-50 text-amber-800 ring-2 ring-amber-200 shadow-md' 
                : 'border-amber-200 bg-amber-50/50 text-amber-700 hover:bg-amber-50/90 hover:border-amber-300';
            } else if (isSelected) {
              statusColor = 'border-gray-400 bg-gray-100 text-gray-800 ring-2 ring-gray-200 shadow-md';
            }

            return (
              <React.Fragment key={stage.id}>
                {/* Node */}
                <button
                  type="button"
                  onClick={() => setSelectedStage(isSelected ? null : stage.id)}
                  className={`w-40 p-4 border rounded-xl flex flex-col items-center text-center transition-all duration-200 cursor-pointer ${statusColor}`}
                >
                  <div className="w-7 h-7 rounded-full flex items-center justify-center font-bold text-xs bg-white border border-current shadow-sm mb-2">
                    {idx + 1}
                  </div>
                  <span className="text-xs font-bold tracking-tight mb-1">{stage.name}</span>
                  <span className="text-[10px] opacity-75">
                    {stage.status === 'skipped' ? 'Skipped' : `${stage.rowCount.toLocaleString()} rows`}
                  </span>
                </button>

                {/* Connection arrow (only if not last) */}
                {idx < stages.length - 1 && (
                  <div className="flex flex-col items-center justify-center w-12 h-16 md:h-auto select-none">
                    {/* SVG Connector line */}
                    <svg className="w-12 h-6 hidden md:block overflow-visible">
                      <line
                        x1="0"
                        y1="12"
                        x2="48"
                        y2="12"
                        stroke="#e5e7eb"
                        strokeWidth="3"
                        strokeDasharray={stage.status === 'skipped' ? '5,5' : '0'}
                      />
                      {stage.status !== 'skipped' && (
                        <circle
                          cx="24"
                          cy="12"
                          r="4"
                          className="fill-teal-500 animate-ping origin-center"
                          style={{ transformOrigin: '24px 12px' }}
                        />
                      )}
                      {/* Arrowhead */}
                      <path d="M42,8 L48,12 L42,16" fill="none" stroke="#e5e7eb" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                    
                    {/* Vertical Connector line for mobile */}
                    <div className="w-0.5 h-8 bg-gray-200 md:hidden relative">
                      {stage.status !== 'skipped' && (
                        <span className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-2 h-2 rounded-full bg-teal-500 animate-ping" />
                      )}
                    </div>
                  </div>
                )}
              </React.Fragment>
            );
          })}
        </div>
      </div>

      {/* Selected Node Details Panel (Inspector Sidebar/Drawer effect) */}
      {currentStageInfo ? (
        <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden animate-fadeIn">
          <div className="border-b border-gray-100 p-5 flex justify-between items-center bg-gray-50/50">
            <div>
              <h4 className="text-sm font-bold text-gray-500 uppercase tracking-wider">Lineage Inspector</h4>
              <h3 className="text-lg font-extrabold text-gray-900 mt-1">{currentStageInfo.name}</h3>
            </div>
            <button
              type="button"
              onClick={() => setSelectedStage(null)}
              className="text-gray-400 hover:text-gray-600 font-medium text-xs border border-gray-200 px-3 py-1.5 rounded-lg bg-white shadow-sm"
            >
              Close Inspector
            </button>
          </div>

          <div className="p-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <span className="text-xs font-bold text-gray-400 uppercase">Description</span>
                <p className="text-sm text-gray-700 mt-1 leading-relaxed">
                  {currentStageInfo.description}
                </p>
              </div>

              <div>
                <span className="text-xs font-bold text-gray-400 uppercase">Stage Metrics</span>
                <div className="grid grid-cols-2 gap-3 mt-2">
                  {Object.entries(currentStageInfo.metrics).map(([key, value]) => (
                    <div key={key} className="bg-gray-50 border border-gray-100 rounded-xl p-3">
                      <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider block">{key}</span>
                      <span className="text-sm font-bold text-gray-900 mt-1 block">{value}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div>
              <span className="text-xs font-bold text-gray-400 uppercase block mb-2">Raw Lineage Event Metadata</span>
              {currentStageInfo.rawEvent ? (
                <div className="space-y-2">
                  <div className="text-xs bg-gray-50 border border-gray-100 rounded-xl p-4 space-y-2.5">
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <span className="text-[9px] text-gray-400 font-bold uppercase block">Timestamp</span>
                        <span className="text-xs text-gray-700 font-mono">{new Date(currentStageInfo.rawEvent.timestamp).toLocaleTimeString()}</span>
                      </div>
                      <div>
                        <span className="text-[9px] text-gray-400 font-bold uppercase block">Reason</span>
                        <span className="text-xs text-gray-700">{currentStageInfo.rawEvent.reason}</span>
                      </div>
                    </div>
                    {currentStageInfo.rawEvent.details && (
                      <div>
                        <span className="text-[9px] text-gray-400 font-bold uppercase block mb-1">Details JSON</span>
                        <pre className="bg-white border border-gray-200 rounded-lg p-2.5 text-[10px] font-mono overflow-x-auto text-gray-800 max-h-48">
                          {JSON.stringify(currentStageInfo.rawEvent.details, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="bg-gray-50 border border-gray-100 rounded-xl p-4 text-xs text-gray-500 italic text-center">
                  No direct raw lineage event logged for this stage. Metadata parsed from report config.
                </div>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="border border-dashed border-gray-200 rounded-2xl p-6 text-center text-sm text-gray-500">
          💡 Click a pipeline stage node above to inspect its metrics and operations.
        </div>
      )}
    </div>
  );
}
