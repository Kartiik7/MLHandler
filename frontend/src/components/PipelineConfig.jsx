import { useState } from 'react';
import { API_BASE } from '../config';

const DEFAULT_YAML = `pipeline:
  name: "My cleaning pipeline"
  version: "1.0"
  steps:
    - step: impute
      strategy: median
      apply_to: numeric

    - step: deduplicate
      subset: all

    - step: trim_whitespace
      apply_to: all

    - step: remove_outliers
      method: iqr
      threshold: 1.5

    - step: type_inference
      enabled: true

    - step: drop_empty_columns
      threshold: 0.9
`;

export default function PipelineConfig({
  cleaningOptions,
  onOptionChange,
  mode,
  setMode,
  pipelineYaml,
  setPipelineYaml,
  disabled,
}) {
  const [validation, setValidation] = useState(null);

  const validateYaml = async () => {
    setValidation({ loading: true });
    try {
      const response = await fetch(`${API_BASE}/validate-yaml`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ yaml: pipelineYaml }),
      });

      const payload = await response.json();
      if (!response.ok) {
        setValidation({ loading: false, valid: false, message: payload.error || 'Invalid YAML' });
        return;
      }

      setValidation({ loading: false, valid: true, message: `Valid YAML (${payload.steps.length} steps)` });
    } catch (err) {
      setValidation({ loading: false, valid: false, message: err.message || 'Validation failed' });
    }
  };

  const downloadYaml = () => {
    const blob = new Blob([pipelineYaml], { type: 'application/x-yaml' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'pipeline-config.yaml';
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="cleaning-options">
      <div className="flex gap-2 mb-4">
        <button
          type="button"
          className={mode === 'simple' ? 'submit-btn' : 'chart-type-toggle'}
          onClick={() => setMode('simple')}
          disabled={disabled}
        >
          Simple Mode
        </button>
        <button
          type="button"
          className={mode === 'yaml' ? 'submit-btn' : 'chart-type-toggle'}
          onClick={() => {
            setMode('yaml');
            if (!pipelineYaml) setPipelineYaml(DEFAULT_YAML);
          }}
          disabled={disabled}
        >
          YAML Mode
        </button>
      </div>

      {mode === 'simple' ? (
        <>
          <h3>Cleaning Options</h3>

          <div className="option-group">
            <label htmlFor="fill_numeric">Fill strategy for numeric columns:</label>
            <select
              id="fill_numeric"
              value={cleaningOptions.fill_missing_numeric}
              onChange={(e) => onOptionChange('fill_missing_numeric', e.target.value)}
              disabled={disabled}
            >
              <option value="mean">Mean</option>
              <option value="median">Median</option>
              <option value="mode">Mode</option>
            </select>
          </div>

          <div className="option-group">
            <label htmlFor="fill_categorical">Fill strategy for categorical columns:</label>
            <select
              id="fill_categorical"
              value={cleaningOptions.fill_missing_categorical}
              onChange={(e) => onOptionChange('fill_missing_categorical', e.target.value)}
              disabled={disabled}
            >
              <option value="mode">Mode</option>
              <option value="Unknown">Unknown</option>
              <option value="None">None</option>
            </select>
          </div>

          <div className="checkboxes">
            <label>
              <input
                type="checkbox"
                checked={cleaningOptions.drop_duplicates}
                onChange={(e) => onOptionChange('drop_duplicates', e.target.checked)}
                disabled={disabled}
              />
              Drop duplicates
            </label>

            <label>
              <input
                type="checkbox"
                checked={cleaningOptions.trim_strings}
                onChange={(e) => onOptionChange('trim_strings', e.target.checked)}
                disabled={disabled}
              />
              Trim whitespace
            </label>

            <label>
              <input
                type="checkbox"
                checked={cleaningOptions.drop_empty_columns}
                onChange={(e) => onOptionChange('drop_empty_columns', e.target.checked)}
                disabled={disabled}
              />
              Drop empty columns
            </label>

            <label>
              <input
                type="checkbox"
                checked={cleaningOptions.convert_types}
                onChange={(e) => onOptionChange('convert_types', e.target.checked)}
                disabled={disabled}
              />
              Convert types
            </label>
          </div>
        </>
      ) : (
        <div className="space-y-3">
          <h3>Pipeline YAML</h3>
          <textarea
            value={pipelineYaml}
            onChange={(e) => setPipelineYaml(e.target.value)}
            disabled={disabled}
            className="w-full min-h-[320px] p-3 border border-gray-300 rounded-md font-mono text-sm"
          />

          <div className="flex gap-3">
            <button type="button" onClick={validateYaml} disabled={disabled || validation?.loading} className="submit-btn">
              {validation?.loading ? 'Validating...' : 'Validate YAML'}
            </button>
            <button type="button" onClick={downloadYaml} disabled={disabled} className="download-btn">
              Download this config
            </button>
          </div>

          {validation && !validation.loading && (
            <p className={validation.valid ? 'status-message success' : 'status-message error'}>{validation.message}</p>
          )}
        </div>
      )}
    </div>
  );
}
