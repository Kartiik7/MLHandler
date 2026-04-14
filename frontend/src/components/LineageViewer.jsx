import { useState } from 'react';

const ACTION_STYLES = {
  file_loaded: 'bg-green-100 text-green-800 border-green-200',
  columns_renamed: 'bg-green-100 text-green-800 border-green-200',
  nulls_imputed: 'bg-blue-100 text-blue-800 border-blue-200',
  types_converted: 'bg-blue-100 text-blue-800 border-blue-200',
  outliers_removed: 'bg-orange-100 text-orange-800 border-orange-200',
  rows_dropped: 'bg-red-100 text-red-800 border-red-200',
};

function getActionClass(action) {
  return ACTION_STYLES[action] || 'bg-gray-100 text-gray-800 border-gray-200';
}

export default function LineageViewer({ lineage }) {
  const [expanded, setExpanded] = useState({});

  if (!lineage || !Array.isArray(lineage.events)) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-gray-600">
        No lineage data available for this task.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-gray-600">
        Total events: <span className="font-semibold text-gray-900">{lineage.total_events}</span>
      </div>

      {lineage.events.map((event, idx) => {
        const key = `${event.timestamp}-${idx}`;
        const isOpen = !!expanded[key];

        return (
          <div key={key} className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
            <div className="flex flex-wrap items-center gap-2 mb-3">
              <span className="text-xs text-gray-500">{event.timestamp}</span>
              <span className={`text-xs px-2 py-1 rounded-full border font-semibold uppercase ${getActionClass(event.action)}`}>
                {event.action}
              </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
              <div>
                <p className="text-gray-500">Column</p>
                <p className="font-medium text-gray-900">{event.column || 'All columns'}</p>
              </div>
              <div>
                <p className="text-gray-500">Affected</p>
                <p className="font-medium text-gray-900">{event.affected_count} rows affected</p>
              </div>
              <div>
                <p className="text-gray-500">Reason</p>
                <p className="font-medium text-gray-900">{event.reason}</p>
              </div>
            </div>

            <button
              type="button"
              onClick={() => setExpanded((prev) => ({ ...prev, [key]: !prev[key] }))}
              className="mt-3 text-sm font-medium text-indigo-600 hover:text-indigo-800"
            >
              {isOpen ? 'Hide Details' : 'Show Details'}
            </button>

            {isOpen && (
              <pre className="mt-3 bg-gray-50 border border-gray-200 rounded-lg p-3 text-xs overflow-x-auto text-gray-800">
                {JSON.stringify(event.details ?? {}, null, 2)}
              </pre>
            )}
          </div>
        );
      })}
    </div>
  );
}
