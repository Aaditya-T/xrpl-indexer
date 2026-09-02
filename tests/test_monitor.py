import json

import monitor


def test_read_meminfo_and_host_metrics(tmp_path, monkeypatch):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal:       1000000 kB\n"
        "MemAvailable:    250000 kB\n"
        "SwapTotal:       500000 kB\n"
        "SwapFree:        400000 kB\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(monitor.shutil, "disk_usage", lambda _path: monitor.shutil._ntuple_diskusage(1000, 250, 750))

    result = monitor.host_metrics(str(meminfo), "/")

    assert result["memory_used_percent"] == 75.0
    assert result["memory_available_mb"] == 244.1
    assert result["swap_total_mb"] == 488.3
    assert result["disk_used_percent"] == 25.0


def test_collect_marks_endpoint_failure_as_warning(monkeypatch):
    monkeypatch.setattr(
        monitor,
        "host_metrics",
        lambda: {"memory_used_percent": 10, "disk_used_percent": 20},
    )
    monkeypatch.setattr(monitor, "check_url", lambda _url: {"ok": False, "error": "down"})
    monkeypatch.delenv("MONITOR_PUBLIC_HEALTH_URL", raising=False)

    result = monitor.collect()

    assert result["level"] == "warning"
    assert result["problems"] == ["local API unavailable", "indexer status unavailable"]
    json.dumps(result)


def test_rejects_non_http_monitor_urls():
    result = monitor.check_url("file:///etc/passwd")

    assert result["ok"] is False
    assert "http" in result["error"]


def test_stale_ledger_is_reported(monkeypatch):
    monkeypatch.setenv("MONITOR_INDEXER_STALE_SECONDS", "60")
    state = {"ledger": 123, "last_progress_at": 10.0}
    record = {
        "level": "ok",
        "problems": [],
        "indexer_status": {"ok": True, "response": {"last_processed_ledger_index": 123}},
    }

    monitor.apply_staleness_check(record, state, now=71.0)

    assert record["level"] == "warning"
    assert record["problems"] == ["indexer ledger has not advanced for 61s"]
