/**
 * ProgressBar — displays a colour-coded progress bar with stage label.
 *
 * Props:
 *   percent  (number)  0–100
 *   stage    (string)  human-readable stage name
 *   status   (string)  'idle' | 'pending' | 'processing' | 'done' | 'error'
 */
export function ProgressBar({ percent, stage, status }) {
  if (status === "idle") return null;

  const colorMap = {
    pending: "#f59e0b",    // amber
    processing: "#3b82f6", // blue
    done: "#22c55e",       // green
    error: "#ef4444",      // red
  };

  const barColor = colorMap[status] || "#3b82f6";

  const containerStyle = {
    width: "100%",
    marginTop: "1rem",
    fontFamily: "inherit",
  };

  const labelRowStyle = {
    display: "flex",
    justifyContent: "space-between",
    fontSize: "0.85rem",
    color: "#6b7280",
    marginBottom: "0.35rem",
  };

  const trackStyle = {
    width: "100%",
    backgroundColor: "#e5e7eb",
    borderRadius: "9999px",
    height: "10px",
    overflow: "hidden",
  };

  const fillStyle = {
    height: "100%",
    borderRadius: "9999px",
    backgroundColor: barColor,
    width: `${Math.min(100, Math.max(0, percent))}%`,
    transition: "width 0.5s ease, background-color 0.3s ease",
  };

  const statusBadgeStyle = {
    display: "inline-block",
    marginTop: "0.5rem",
    padding: "0.15rem 0.6rem",
    borderRadius: "9999px",
    fontSize: "0.75rem",
    fontWeight: 600,
    backgroundColor: barColor + "22",
    color: barColor,
    border: `1px solid ${barColor}44`,
    textTransform: "uppercase",
    letterSpacing: "0.04em",
  };

  return (
    <div style={containerStyle}>
      <div style={labelRowStyle}>
        <span>{stage}</span>
        <span>{percent}%</span>
      </div>
      <div style={trackStyle}>
        <div style={fillStyle} />
      </div>
      <div>
        <span style={statusBadgeStyle}>{status}</span>
      </div>
    </div>
  );
}
