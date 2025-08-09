# stream_processor/util.py

from datetime import datetime, timezone
from typing import Any

from datetime import datetime, timezone
from typing import Any

def parse_event_ts_utc(value: Any) -> datetime:
    """
    Accepts ISO8601 string or epoch seconds/millis and returns UTC-aware datetime.
    """
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        # Heuristic: treat > 1e10 as ms
        if value > 1e10:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()