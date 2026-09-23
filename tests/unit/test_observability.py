import json
import logging
import sys
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from switch_pipeline.observability import bound_contextvars, configure_logging, get_logger
from switch_pipeline.settings import LogSettings


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def json_lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]


def test_logs_are_json_with_service_and_correlation_ids(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(
        LogSettings(level="INFO", format="json"), service="consumer", stream=sys.stdout
    )
    with bound_contextvars(batch_id="batch-1", event_id="event-1"):
        get_logger("switch_pipeline.test").info("event_quarantined", reason="schema_violation")
        logging.getLogger("some.library").warning("library message")
    get_logger("switch_pipeline.test").debug("filtered by level")

    ours, library = json_lines(capsys)
    assert ours["event"] == "event_quarantined"
    assert (ours["service"], ours["batch_id"], ours["event_id"]) == (
        "consumer",
        "batch-1",
        "event-1",
    )
    assert (ours["level"], ours["reason"]) == ("info", "schema_violation")
    assert ours["timestamp"].endswith("Z")
    assert (library["event"], library["logger"], library["batch_id"]) == (
        "library message",
        "some.library",
        "batch-1",
    )


def test_tracebacks_are_structured_without_frame_locals(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(
        LogSettings(level="INFO", format="json"), service="adapter", stream=sys.stdout
    )
    secret_payload = {"o_comment": "must not reach the logs"}
    try:
        raise ValueError(f"bad row {len(secret_payload)}")
    except ValueError:
        get_logger("switch_pipeline.test").exception("sync_cycle_failed")

    [line] = json_lines(capsys)
    [exception] = line["exception"]
    assert (exception["exc_type"], exception["exc_value"]) == ("ValueError", "bad row 1")
    assert all("locals" not in frame for frame in exception["frames"])
    assert "must not reach the logs" not in json.dumps(line)


def test_console_format_is_human_readable(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(LogSettings(level="INFO", format="console"), service="api", stream=sys.stdout)
    get_logger("switch_pipeline.test").info("api_started", port=8000)
    output = capsys.readouterr().out
    assert "api_started" in output
    assert "port=8000" in output
