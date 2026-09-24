"""Consumer entry point: materializes the topic into PostgreSQL."""

from psycopg_pool import ConnectionPool

from switch_pipeline.consumer.processor import EventProcessor
from switch_pipeline.consumer.runner import ConsumerRunner
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import configure_logging, get_logger
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import (
    ConsumerSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    load_settings,
)
from switch_pipeline.transport.admin import TopicAdmin

log = get_logger(__name__)


def open_sink_pool(postgres: PostgresSettings, *, application_name: str) -> ConnectionPool:
    pool = ConnectionPool(
        postgres.conninfo(application_name=application_name),
        min_size=1,
        max_size=1,
        open=False,
        check=ConnectionPool.check_connection,
        name="sink",
    )
    pool.open(wait=True, timeout=float(postgres.connect_timeout_seconds))
    return pool


def run_consumer() -> int:
    kafka = load_settings(KafkaSettings)
    settings = load_settings(ConsumerSettings)
    postgres = load_settings(PostgresSettings)
    configure_logging(load_settings(LogSettings), service="consumer")

    rules = load_rules(settings.quality_rules_path)
    log.info(
        "quality_rules_loaded",
        path=str(rules.path),
        fingerprint=rules.fingerprint,
        rules_per_entity=rules.summary(),
    )
    shutdown = Shutdown().install_signal_handlers()
    TopicAdmin(kafka, client_id="switch-consumer-admin").require_topic()
    pool = open_sink_pool(postgres, application_name="switch-consumer")
    try:
        ConsumerRunner(
            kafka=kafka,
            settings=settings,
            pool=pool,
            processor=EventProcessor(rules),
            shutdown=shutdown,
            heartbeat=Heartbeat(settings.heartbeat_path),
            client_id="switch-consumer",
        ).run()
    finally:
        pool.close()
    return 0
