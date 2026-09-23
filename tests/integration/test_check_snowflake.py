"""`switch-pipeline check-snowflake` against the emulator."""

import socket
from pathlib import Path

from switch_pipeline.settings import SeedSettings, SnowflakeSettings
from switch_pipeline.tools.check import check_snowflake
from switch_pipeline.tools.seed import seed_source
from tests.integration.conftest import SOURCE_SETTINGS, synthetic_seed


def statuses(results: list) -> list[tuple[str, bool]]:  # type: ignore[type-arg]
    return [(result.name, result.ok) for result in results]


def test_missing_objects_are_reported_with_hints_until_the_table_is_seeded(
    snowflake_settings: SnowflakeSettings,
) -> None:
    before = check_snowflake(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(10))
    # The emulator creates the session's database on sign-in, so only the table is missing.
    assert [name for name, _ in statuses(before)] == ["sign_in", "database", "source_table"]
    assert before[-1].ok is False
    assert "make seed" in (before[-1].hint or "")

    seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(10), force=False)
    after = check_snowflake(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(10))
    assert all(result.ok for result in after)
    assert "10 rows" in after[-1].detail


def test_the_sample_share_is_checked_when_seeding_from_it(
    snowflake_settings: SnowflakeSettings,
) -> None:
    seed = SeedSettings(
        strategy="sample_share", sample_schema="SNOWFLAKE_SAMPLE_DATA.TPCH_SF1", row_count=10
    )
    share = check_snowflake(snowflake_settings, SOURCE_SETTINGS, seed)[-1]
    assert (share.name, share.ok) == ("sample_share", False)
    assert "IMPORTED PRIVILEGES" in (share.hint or "")


def test_a_missing_private_key_is_reported_before_signing_in(
    snowflake_settings: SnowflakeSettings, tmp_path: Path
) -> None:
    settings = snowflake_settings.model_copy(
        update={"password": None, "private_key_path": tmp_path / "missing.p8"}
    )
    [result] = check_snowflake(settings, SOURCE_SETTINGS, synthetic_seed(1))
    assert (result.name, result.ok) == ("private_key", False)
    assert "make snowflake-keypair" in (result.hint or "")


def test_an_unreachable_endpoint_points_at_the_account_identifier(
    snowflake_settings: SnowflakeSettings,
) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    settings = snowflake_settings.model_copy(
        update={"port": closed_port, "login_timeout_seconds": 3, "network_timeout_seconds": 3}
    )
    [result] = check_snowflake(settings, SOURCE_SETTINGS, synthetic_seed(1))
    assert (result.name, result.ok) == ("sign_in", False)
    assert "SNOWFLAKE_ACCOUNT" in (result.hint or "")
