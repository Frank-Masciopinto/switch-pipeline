"""Sink: everything PostgreSQL (migrations, materialization, sync state, API reads).

Depends only on the domain and on the ports it implements; knows nothing
about Kafka or Snowflake. Connection failures surface as
DatabaseUnavailableError, never as driver exceptions.
"""
