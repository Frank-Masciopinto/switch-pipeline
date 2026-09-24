"""The error policy every component follows.

- ``RetryableError``: a dependency is unavailable right now (network, broker,
  database, a source table that does not exist yet). Workers retry with backoff.
- ``FatalPipelineError``: retrying cannot fix it (misconfiguration, a broken
  contract). The worker stops and the process exits non-zero.
- ``ConfigurationError``: .env or the quality rules file is invalid; reported
  before any work starts (exit code 2).
- Anything else is a bug or a failure nobody anticipated. It is not retried
  either: the worker stops and logs the traceback.

State only advances after a durable write, so stopping never loses data.
"""


class ConfigurationError(Exception):
    """Settings or the quality rules file are missing or invalid."""


class RetryableError(Exception):
    """A dependency is unavailable right now; the same operation can succeed later."""


class SourceUnavailableError(RetryableError):
    """Snowflake cannot be read: unreachable, or the source table does not exist yet."""


class BrokerUnavailableError(RetryableError):
    """Kafka did not confirm the delivery of every event."""


class DatabaseUnavailableError(RetryableError):
    """PostgreSQL cannot be reached."""


class FatalPipelineError(Exception):
    """Retrying cannot fix this (misconfiguration, a broken contract): stop the worker."""


class RecordRejectedError(Exception):
    """The sink cannot store this one record (e.g. a value its types reject)."""
