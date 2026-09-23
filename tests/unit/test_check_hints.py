import pytest

from switch_pipeline.tools.check import sign_in_hint


@pytest.mark.parametrize(
    ("message", "points_at"),
    [
        ("250001: Failed to load private key: Bad decrypt", "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        ("390144 (08004): JWT token is invalid.", "RSA_PUBLIC_KEY"),
        ("394507: Multi-factor authentication is required for this account", "MFA"),
        ("390100 (08004): Incorrect username or password was specified.", "SNOWFLAKE_PASSWORD"),
        (
            "Role 'SWITCH_PIPELINE_ROLE' specified in the connect string does not exist or not "
            "authorized.",
            "SNOWFLAKE_ROLE",
        ),
        ("250001 (08001): Failed to connect to DB: xy12345.snowflakecomputing.com:443", "ACCOUNT"),
        ("404 Not Found", "SNOWFLAKE_ACCOUNT"),
    ],
)
def test_sign_in_failures_map_to_the_most_likely_fix(message: str, points_at: str) -> None:
    assert points_at in sign_in_hint(message)
