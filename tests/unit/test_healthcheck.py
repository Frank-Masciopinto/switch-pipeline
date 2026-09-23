import os
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from switch_pipeline.healthcheck import main


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200 if self.path == "/ready" else 503)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def server() -> Iterator[str]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_a_fresh_heartbeat_is_healthy(tmp_path: Path) -> None:
    beat = tmp_path / "heartbeat"
    beat.touch()
    assert main(["heartbeat", str(beat), "--max-age", "60"]) == 0


def test_a_stale_heartbeat_is_unhealthy(tmp_path: Path) -> None:
    beat = tmp_path / "heartbeat"
    beat.touch()
    two_minutes_ago = time.time() - 120
    os.utime(beat, (two_minutes_ago, two_minutes_ago))
    assert main(["heartbeat", str(beat), "--max-age", "60"]) == 1


def test_a_worker_that_never_beat_is_unhealthy(tmp_path: Path) -> None:
    assert main(["heartbeat", str(tmp_path / "missing"), "--max-age", "60"]) == 1


def test_http_probe_passes_only_on_200(server: str) -> None:
    assert main(["http", f"{server}/ready"]) == 0
    assert main(["http", f"{server}/not-ready"]) == 1


def test_http_probe_fails_when_nothing_listens() -> None:
    assert main(["http", "http://127.0.0.1:9/ready", "--timeout", "1"]) == 1
