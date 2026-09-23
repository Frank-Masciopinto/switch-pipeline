"""Snowflake -> Kafka -> PostgreSQL change-data pipeline with an event-inspection API."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("switch-pipeline")
except PackageNotFoundError:  # source checkout that was never installed
    __version__ = "0.0.0"
