"""Small host and endpoint monitor intended to run under PM2 on the EC2 host."""

from __future__ import annotations

import json
import os
import shutil
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dotenv import load_dotenv


load_dotenv()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def read_meminfo(path: str = "/proc/meminfo") -> dict[str, int]:
    """Return Linux /proc/meminfo values in KiB."""
    values: dict[str, int] = {}
    try:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            token = raw.strip().split()[0]
            values[key] = int(token)
    except (OSError, ValueError, IndexError):
        return {}
    return values


def host_metrics(meminfo_path: str = "/proc/meminfo", disk_path: str = "/") -> dict[str, Any]:
    memory = read_meminfo(meminfo_path)
    total_kib = memory.get("MemTotal", 0)
    available_kib = memory.get("MemAvailable", memory.get("MemFree", 0))
    used_percent = round((1 - available_kib / total_kib) * 100, 1) if total_kib else None

    disk = shutil.disk_usage(disk_path)
    disk_percent = round(disk.used / disk.total * 100, 1) if disk.total else None

    try:
        load_1m, load_5m, load_15m = (round(value, 2) for value in os.getloadavg())
    except OSError:
        load_1m = load_5m = load_15m = None

    return {
        "memory_used_percent": used_percent,
        "memory_available_mb": round(available_kib / 1024, 1) if total_kib else None,
        "swap_total_mb": round(memory.get("SwapTotal", 0) / 1024, 1),
        "swap_free_mb": round(memory.get("SwapFree", 0) / 1024, 1),
        "disk_used_percent": disk_percent,
        "disk_free_gb": round(disk.free / (1024**3), 2),
        "load_1m": load_1m,
        "load_5m": load_5m,
        "load_15m": load_15m,
    }


def check_url(url: str, timeout: float = 8.0) -> dict[str, Any]:
    started = time.monotonic()
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("monitor URLs must use http:// or https://")
        request = Request(url, headers={"User-Agent": "xrpl-indexer-monitor/1.0"})
        with urlopen(request, timeout=timeout) as response:  # nosec B310: scheme is restricted above
            body = response.read(4096)
            status = response.status
        try:
            payload: Any = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = body.decode("utf-8", errors="replace")[:200]
        return {
            "ok": 200 <= status < 300,
            "status": status,
            "latency_ms": round((time.monotonic() - started) * 1000),
            "response": payload,
        }
    except HTTPError as error:
        reason = f"HTTP {error.code}"
    except (URLError, TimeoutError, OSError, ValueError) as error:
        reason = str(error.reason) if isinstance(error, URLError) else str(error)
    return {
        "ok": False,
        "latency_ms": round((time.monotonic() - started) * 1000),
        "error": reason,
    }


def collect() -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        **host_metrics(),
    }
    local_url = os.getenv("MONITOR_LOCAL_HEALTH_URL", "http://127.0.0.1:8000/health")
    record["local_api"] = check_url(local_url)
    status_url = os.getenv("MONITOR_LOCAL_STATUS_URL", "http://127.0.0.1:8000/status")
    record["indexer_status"] = check_url(status_url)

    public_url = os.getenv("MONITOR_PUBLIC_HEALTH_URL", "").strip()
    if public_url:
        record["public_api"] = check_url(public_url)

    memory_warn = _env_float("MONITOR_MEMORY_WARN_PERCENT", 85)
    disk_warn = _env_float("MONITOR_DISK_WARN_PERCENT", 85)
    problems = []
    if record["memory_used_percent"] is not None and record["memory_used_percent"] >= memory_warn:
        problems.append(f"memory at {record['memory_used_percent']}%")
    if record["disk_used_percent"] is not None and record["disk_used_percent"] >= disk_warn:
        problems.append(f"disk at {record['disk_used_percent']}%")
    if not record["local_api"]["ok"]:
        problems.append("local API unavailable")
    if not record["indexer_status"]["ok"]:
        problems.append("indexer status unavailable")
    if "public_api" in record and not record["public_api"]["ok"]:
        problems.append("public API unavailable")
    record["level"] = "warning" if problems else "ok"
    record["problems"] = problems
    return record


def apply_staleness_check(
    record: dict[str, Any], state: dict[str, Any], now: float | None = None
) -> None:
    """Warn when the indexer ledger has not advanced for the configured period."""
    current_time = time.monotonic() if now is None else now
    response = record.get("indexer_status", {}).get("response")
    ledger = response.get("last_processed_ledger_index") if isinstance(response, dict) else None
    if not isinstance(ledger, int):
        return

    if state.get("ledger") != ledger:
        state["ledger"] = ledger
        state["last_progress_at"] = current_time
        return

    stale_after = max(_env_float("MONITOR_INDEXER_STALE_SECONDS", 900), 60)
    last_progress = state.setdefault("last_progress_at", current_time)
    if current_time - last_progress >= stale_after:
        record["level"] = "warning"
        record["problems"].append(f"indexer ledger has not advanced for {round(current_time - last_progress)}s")


def send_webhook(webhook_url: str, record: dict[str, Any], recovered: bool = False) -> None:
    state = "RECOVERED" if recovered else "ALERT"
    summary = ", ".join(record.get("problems", [])) or "all checks healthy"
    message = f"XRPL indexer {state} on {record['host']}: {summary}"
    payload = json.dumps({"text": message, "record": record}).encode("utf-8")
    try:
        parsed = urlparse(webhook_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("webhook URL must use http:// or https://")
        request = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=8) as response:  # nosec B310: scheme is restricted above
            response.read(1024)
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
        print(json.dumps({"level": "warning", "webhook_error": str(error)}), flush=True)


def main() -> None:
    interval = max(_env_float("MONITOR_INTERVAL_SECONDS", 60), 10)
    webhook_url = os.getenv("MONITOR_WEBHOOK_URL", "").strip()
    previous_warning = False
    progress_state: dict[str, Any] = {}

    while True:
        record = collect()
        apply_staleness_check(record, progress_state)
        print(json.dumps(record, separators=(",", ":"), default=str), flush=True)
        warning = record["level"] == "warning"
        if webhook_url and warning != previous_warning:
            send_webhook(webhook_url, record, recovered=not warning)
        previous_warning = warning
        time.sleep(interval)


if __name__ == "__main__":
    main()
