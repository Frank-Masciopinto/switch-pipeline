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
from switch_pipeline.transport.consumer import KafkaStreams

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
    streams = KafkaStreams(kafka, client_id="switch-consumer")
    with (
        closing(PostgresSink.open(postgres, application_name="switch-consumer")) as sink,
        streams.live() as stream,
    ):
        log.info("consumer_started", topic=kafka.topic, group=kafka.consumer_group)
        ConsumerRunner(
            sink=sink,
            processor=EventProcessor(rules),
            settings=settings,
            shutdown=shutdown,
            heartbeat=Heartbeat(settings.heartbeat_path),
        ).run(stream)
    log.info("consumer_stopped")
    return 0
