import { useState, useEffect, useRef } from "react";
import { WS_BASE } from "../config";

/**
 * useTaskProgress — opens a WebSocket to /ws/{taskId} and streams
 * { percent, stage, status } progress events from the Celery worker.
 *
 * Returns:
 *   progress  — { percent: number, stage: string, status: string }
 *   result    — { downloadUrl: string, report: object } | null  (set on completion)
 *   error     — string | null
 */
export function useTaskProgress(taskId) {
  const [progress, setProgress] = useState({
    percent: 0,
    stage: "Idle",
    status: "idle",
  });
  const [result, setResult] = useState(null);
  const [wsError, setWsError] = useState(null);
  const wsRef = useRef(null);

  useEffect(() => {
    if (!taskId) return;

    // Reset state for a new task
    setProgress({ percent: 0, stage: "Queued", status: "pending" });
    setResult(null);
    setWsError(null);

    const ws = new WebSocket(`${WS_BASE}/ws/${taskId}`);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log(`[WS] Connected for task ${taskId}`);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        setProgress({
          percent: data.percent ?? 0,
          stage: data.stage ?? "Processing",
          status: data.status ?? "processing",
        });

        if (data.status === "done") {
          setResult({
            download_url: data.download_url,
            download_url_parquet: data.download_url_parquet,
            download_url_excel: data.download_url_excel,
            lineage_url: data.lineage_url,
            report: data.report ?? {},
          });
          ws.close();
        }

        if (data.status === "error") {
          setWsError(data.error ?? "Processing failed");
          ws.close();
        }
      } catch (err) {
        console.error("[WS] Failed to parse message:", err);
      }
    };

    ws.onerror = (e) => {
      console.error("[WS] Connection error:", e);
      setWsError("WebSocket connection failed — is the server running?");
      setProgress((prev) => ({
        ...prev,
        status: "error",
        stage: "Connection failed",
      }));
    };

    ws.onclose = (e) => {
      console.log(`[WS] Closed for task ${taskId} (code ${e.code})`);
    };

    // Cleanup: close socket when taskId changes or component unmounts
    return () => {
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        ws.close();
      }
    };
  }, [taskId]);

  return { progress, result, wsError };
}
