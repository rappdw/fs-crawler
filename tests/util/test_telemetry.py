import json
from pathlib import Path

from fscrawler.util.telemetry import TelemetryEmitter


def test_telemetry_emitter_writes_json(tmp_path):
    path = tmp_path / "telemetry.log"
    emitter = TelemetryEmitter(path=str(path))
    emitter.emit("test_event", value=42)
    emitter.close()

    contents = path.read_text().strip().splitlines()
    assert len(contents) == 1
    record = json.loads(contents[0])
    assert record["event"] == "test_event"
    assert record["value"] == 42
    assert "ts" in record
