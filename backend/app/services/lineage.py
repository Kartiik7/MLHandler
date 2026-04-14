import json
import math
from datetime import datetime
from typing import Any, Dict, List, Optional


class LineageTracker:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []

    def log(
        self,
        action: str,
        reason: str,
        count: int,
        column: Optional[str] = None,
        details: Any = None,
    ) -> None:
        """Log any pipeline action that modifies the dataframe."""
        self.events.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "action": action,
                "column": column,
                "reason": reason,
                "affected_count": count,
                "details": details,
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"total_events": len(self.events), "events": self._sanitize(self.events)}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, allow_nan=False)

    def _sanitize(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: self._sanitize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize(v) for v in value]

        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                pass

        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass

        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, (str, int, bool)) or value is None:
            return value
        return str(value)
