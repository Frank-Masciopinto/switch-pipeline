import json
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from switch_pipeline import __version__, cli
from switch_pipeline.errors import FatalPipelineError
from tests.helpers import REPO_ROOT, read_env_example


@pytest.fixture
def no_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """No inherited variables and no .env in the working directory."""
    for name in read_env_example():
        monkeypatch.delenv(name, raising=False)
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


@pytest.mark.parametrize(
    ("error", "event"),
    [
        (FatalPipelineError("broken contract"), "fatal_error"),
        (RuntimeError("bug"), "unhandled_error"),
    ],
)
def test_failures_are_logged_as_structured_errors_and_exit_1(
    monkeypatch: pytest.MonkeyPatch, error: Exception, event: str
) -> None:
    def fail(_: object) -> int:
        raise error

    monkeypatch.setattr(cli, "_export_schema", fail)
    with capture_logs() as logs:
        assert cli.main(["export-schema"]) == 1
    assert (logs[-1]["event"], logs[-1]["log_level"], logs[-1]["exc_info"]) == (
        event,
        "error",
        True,
    )


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        cli.main(["--version"])
    assert exited.value.code == 0
    assert capsys.readouterr().out.strip() == __version__
