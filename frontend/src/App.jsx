import React, { useEffect, useState } from 'react';
import axios from 'axios';

import DataVisualizations from './components/DataVisualizations';
import LineageViewer from './components/LineageViewer';
import PipelineConfig from './components/PipelineConfig';
import { ProgressBar } from './components/ProgressBar';
import { useTaskProgress } from './hooks/useTaskProgress';
import { API_BASE } from './config';
import './App.css';

function App() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');

  const [report, setReport] = useState(null);
  const [columnStats, setColumnStats] = useState(null);
  const [lineage, setLineage] = useState(null);
  const [outlierDownloadUrl, setOutlierDownloadUrl] = useState('');

  const [taskId, setTaskId] = useState(null);
  const [activeTab, setActiveTab] = useState('report');
  const { progress, result, wsError } = useTaskProgress(taskId);

  const [cleaningOptions, setCleaningOptions] = useState({
    fill_missing_numeric: 'median',
    fill_missing_categorical: 'mode',
    drop_duplicates: true,
    trim_strings: true,
    drop_empty_columns: true,
    convert_types: true,
  });

  const [configMode, setConfigMode] = useState('simple');
  const [pipelineYaml, setPipelineYaml] = useState('');

  useEffect(() => {
    if (!result || !taskId) return;

    setReport(result.report || null);
    setStatus('Processing complete!');
    setActiveTab('report');

    const fetchResults = async () => {
      setLoading(true);
      try {
        const statsRes = await fetch(`${API_BASE}/column-stats/${taskId}`);
        if (!statsRes.ok) throw new Error('Stats fetch failed');
        const stats = await statsRes.json();
        setColumnStats(stats);

        if (result.lineage_url) {
          const lineageRes = await fetch(`${API_BASE}${result.lineage_url}`);
          if (lineageRes.ok) {
            const lineagePayload = await lineageRes.json();
            setLineage(lineagePayload);
          }
        }
      } catch (err) {
        console.error('Failed to fetch post-processing data:', err);
        setError('Processing finished but fetching reports failed.');
      } finally {
        setLoading(false);
        setProcessing(false);
      }
    };

    fetchResults();
  }, [result, taskId]);

  useEffect(() => {
    if (!wsError) return;
    setError(wsError);
    setProcessing(false);
  }, [wsError]);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile && selectedFile.name.toLowerCase().endsWith('.csv')) {
      setFile(selectedFile);
      setError('');
    } else {
      setFile(null);
      setError('Please select a valid CSV file');
    }
  };

  const handleOptionChange = (key, value) => {
    setCleaningOptions((prev) => ({ ...prev, [key]: value }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!file) {
      setError('Please select a file');
      return;
    }

    setProcessing(true);
    setStatus('Uploading — queuing task...');
    setError('');
    setReport(null);
    setColumnStats(null);
    setLineage(null);
    setOutlierDownloadUrl('');
    setTaskId(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      if (configMode === 'yaml' && pipelineYaml.trim()) {
        formData.append('pipeline_yaml', pipelineYaml);
      } else {
        formData.append('config', JSON.stringify(cleaningOptions));
      }

      const response = await fetch(`${API_BASE}/upload`, {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || body.error || `Upload failed with status ${response.status}`);
      }

      const { task_id } = await response.json();
      setStatus('Task queued — processing in background...');
      setTaskId(task_id);
    } catch (err) {
      console.error('Upload error:', err);
      setError(err.message || 'Failed to upload CSV');
      setStatus('');
      setProcessing(false);
    }
  };

  const detectOutliers = async () => {
    if (!taskId) {
      return { error: 'No processed task found. Please process a file first.' };
    }

    try {
      const response = await axios.post(`${API_BASE}/api/outliers`, { task_id: taskId });
      return { data: response.data };
    } catch (err) {
      console.error('Outlier detection error:', err);
      return { error: err.response?.data?.error || err.message || 'Failed to detect outliers' };
    }
  };

  const cleanOutliers = async (selectedColumns) => {
    if (!taskId) {
      return { error: 'No processed task found. Please process a file first.' };
    }

    if (!selectedColumns || selectedColumns.length === 0) {
      return { error: 'Please select at least one column for outlier removal.' };
    }

    try {
      setLoading(true);

      const response = await axios.post(`${API_BASE}/api/remove-outliers`, {
        task_id: taskId,
        columns: selectedColumns,
      });

      if (response.data?.download_url) {
        setOutlierDownloadUrl(`${API_BASE}${response.data.download_url}`);
      }

      return {
        success: true,
        stats: {
          rowsBefore: response.data.original_rows,
          rowsAfter: response.data.cleaned_rows,
          rowsRemoved: response.data.rows_removed,
        },
      };
    } catch (err) {
      console.error('Outlier cleaning error:', err);
      return { error: err.response?.data?.error || err.message || 'Failed to clean outliers' };
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>MLHandler — CSV Data Processor</h1>
      </header>

      <main className="app-main">
        <section className="upload-section">
          <form onSubmit={handleSubmit} className="upload-form">
            <div className="form-group">
              <label htmlFor="file">Select CSV File</label>
              <input id="file" type="file" accept=".csv" onChange={handleFileChange} disabled={processing} />
            </div>

            <PipelineConfig
              cleaningOptions={cleaningOptions}
              onOptionChange={handleOptionChange}
              mode={configMode}
              setMode={setConfigMode}
              pipelineYaml={pipelineYaml}
              setPipelineYaml={setPipelineYaml}
              disabled={processing}
            />

            <button type="submit" disabled={!file || processing} className="submit-btn">
              {processing ? 'Processing...' : 'Upload & Process'}
            </button>
          </form>

          {status && <div className="status-message success">{status}</div>}
          {error && <div className="status-message error">{error}</div>}

          <ProgressBar percent={progress.percent} stage={progress.stage} status={progress.status} />

          {report && (
            <div className="report-section">
              <h2>Results</h2>

              <div className="flex gap-3 mt-4">
                <button type="button" className={activeTab === 'report' ? 'submit-btn' : 'download-btn'} onClick={() => setActiveTab('report')}>
                  Report
                </button>
                <button
                  type="button"
                  className={activeTab === 'visualizations' ? 'submit-btn' : 'download-btn'}
                  onClick={() => setActiveTab('visualizations')}
                >
                  Visualizations
                </button>
                <button type="button" className={activeTab === 'audit' ? 'submit-btn' : 'download-btn'} onClick={() => setActiveTab('audit')}>
                  Audit Log
                </button>
              </div>

              {activeTab === 'report' && (
                <>
                  <div className="report-summary">
                    <div className="summary-item">
                      <strong>Rows before:</strong> {report.rows_before}
                    </div>
                    <div className="summary-item">
                      <strong>Rows after:</strong> {report.rows_after}
                    </div>
                    <div className="summary-item">
                      <strong>Columns before:</strong> {report.columns_before}
                    </div>
                    <div className="summary-item">
                      <strong>Columns after:</strong> {report.columns_after}
                    </div>
                    {report.duplicates_removed !== undefined && (
                      <div className="summary-item">
                        <strong>Duplicates removed:</strong> {report.duplicates_removed}
                      </div>
                    )}
                  </div>

                  {taskId && (
                    <div className="flex gap-3 mt-4">
                      <a href={`${API_BASE}/download/${taskId}?format=csv`} download className="download-btn">
                        Download CSV
                      </a>
                      <a href={`${API_BASE}/download/${taskId}?format=parquet`} download className="download-btn">
                        Download Parquet
                      </a>
                      <a href={`${API_BASE}/download/${taskId}?format=excel`} download className="download-btn">
                        Download Excel
                      </a>
                      {outlierDownloadUrl && (
                        <a href={outlierDownloadUrl} download className="download-btn">
                          Download No-Outliers CSV
                        </a>
                      )}
                    </div>
                  )}
                </>
              )}

              {activeTab === 'visualizations' && columnStats && (
                <section className="visualizations-section">
                  <DataVisualizations
                    data={columnStats}
                    report={report}
                    loading={loading}
                    onDetectOutliers={detectOutliers}
                    onCleanOutliers={cleanOutliers}
                  />
                </section>
              )}

              {activeTab === 'audit' && <LineageViewer lineage={lineage} />}
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

export default App;
