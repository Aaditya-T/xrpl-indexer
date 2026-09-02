import json

import observability


def test_emit_produces_timestamped_json(capsys):
    observability.emit("test_event", duration_seconds=1.25)

    record = json.loads(capsys.readouterr().out)
    assert record["event"] == "test_event"
    assert record["level"] == "info"
    assert record["timestamp"].endswith("+00:00")
    assert record["duration_seconds"] == 1.25
    assert isinstance(record["pid"], int)


def test_process_memory_reports_peak():
    result = observability.process_memory_mb()

    assert result["peak_rss_mb"] > 0
