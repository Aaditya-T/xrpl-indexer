"""Structured operational events for PM2 and CloudWatch logs."""

from __future__ import annotations

import json
import os
import resource
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def process_memory_mb() -> dict[str, float | None]:
    """Return current and peak resident memory without adding a dependency."""
    current_mb: float | None = None
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                current_mb = round(int(line.split()[1]) / 1024, 1)
                break
    except (OSError, ValueError, IndexError):
        pass

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB; macOS reports bytes.
    peak_mb = peak / (1024**2) if sys.platform == "darwin" else peak / 1024
    return {"rss_mb": current_mb, "peak_rss_mb": round(peak_mb, 1)}


def emit(event: str, *, level: str = "info", **fields: Any) -> None:
    """Write one timestamped JSON event; PM2 captures stdout line-by-line."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        "pid": os.getpid(),
        **fields,
    }
    print(json.dumps(record, separators=(",", ":"), default=str), flush=True)
