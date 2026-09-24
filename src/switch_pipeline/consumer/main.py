"""Consumer entry point: materializes the topic into PostgreSQL."""

from contextlib import closing

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
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.transport.admin import TopicAdmin

log = get_logger(__name__)


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
    with closing(PostgresSink.open(postgres, application_name="switch-consumer")) as sink:
        ConsumerRunner(
            kafka=kafka,
            settings=settings,
            sink=sink,
            processor=EventProcessor(rules),
            shutdown=shutdown,
            heartbeat=Heartbeat(settings.heartbeat_path),
            client_id="switch-consumer",
        ).run()
    return 0
