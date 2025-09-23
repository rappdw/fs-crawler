import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Optional


class TelemetryEmitter:
    def __init__(self, path: Optional[str] = None, stream: Optional[IO[str]] = None):
        if path is None and stream is None:
            raise ValueError("TelemetryEmitter requires a file path or stream")
        self._path = Path(path) if path else None
        self._stream = stream
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("a", encoding="utf-8")
        else:
            self._file = stream

    def emit(self, event: str, **fields):
        record = {
            "ts": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "event": event,
        }
        record.update(fields)
        json_record = json.dumps(record, separators=(",", ":"))
        self._file.write(json_record + "\n")
        self._file.flush()

    def close(self):
        if self._path and self._file:
            self._file.close()
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def telemetry_from_args(path: Optional[str]) -> Optional[TelemetryEmitter]:
    if not path:
        return None
    if path == "-":
        return TelemetryEmitter(stream=sys.stdout)
    return TelemetryEmitter(path=path)
