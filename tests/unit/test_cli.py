import json
import socket
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from switch_pipeline import __version__, cli
from tests.helpers import REPO_ROOT, exception_types, logged, read_env_example


@pytest.fixture
def no_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty working directory: no .env file."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_missing_configuration_exits_2_naming_each_variable(
    no_config: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["check-config"]) == 2
    errors = capsys.readouterr().err
    assert "SNOWFLAKE_ACCOUNT: Field required" in errors
    assert "KAFKA_TOPIC: Field required" in errors


def test_check_config_accepts_the_template_and_the_rules_file(
    no_config: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (no_config / ".env").write_text((REPO_ROOT / ".env.example").read_text())
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myorg-myaccount")
    monkeypatch.setenv("QUALITY_RULES_PATH", str(REPO_ROOT / "config" / "quality_rules.yaml"))
    assert cli.main(["check-config"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["settings"] == "ok"
    assert report["quality_rules"]["order"] >= 3


def test_export_schema_reproduces_the_committed_schema(tmp_path: Path) -> None:
    output = tmp_path / "schema.json"
    assert cli.main(["export-schema", "--output", str(output)]) == 0
    committed = REPO_ROOT / "schemas" / "change_event.v1.schema.json"
    assert output.read_text() == committed.read_text()


def test_an_unexpected_error_is_logged_with_its_traceback_and_exits_1(tmp_path: Path) -> None:
    with capture_logs() as logs:
        assert cli.main(["export-schema", "--output", str(tmp_path)]) == 1  # a directory
    assert (logs[-1]["event"], logs[-1]["log_level"], logs[-1]["exc_info"]) == (
        "unhandled_error",
        "error",
        True,
    )


def test_an_unreachable_database_is_reported_as_unavailable_and_exits_1(
    entrypoint_state: None,
    no_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with socket.socket() as probe:  # a port nothing listens on
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    for name, value in read_env_example().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", str(closed_port))
    assert cli.main(["migrate"]) == 1
    record = logged(capsys.readouterr().out, "dependency_unavailable")
    assert record["level"] == "error"
    assert "DatabaseUnavailableError" in exception_types(record)


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        cli.main(["--version"])
    assert exited.value.code == 0
    assert capsys.readouterr().out.strip() == __version__
