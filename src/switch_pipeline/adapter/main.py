"""Adapter entry point: wires Snowflake, PostgreSQL state and Kafka together."""

from switch_pipeline.adapter.mapper import EventMapper
from switch_pipeline.adapter.service import SyncService
from switch_pipeline.adapter.source import (
    SnowflakeChangeSource,
    SnowflakeConnectionFactory,
    SourceTable,
)
from switch_pipeline.adapter.state import PostgresSyncStateStore, SourceLock
from switch_pipeline.domain.envelope import SourceRef
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import configure_logging, get_logger
from switch_pipeline.retry import Backoff
from switch_pipeline.settings import (
    AdapterSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    SnowflakeSettings,
    SourceSettings,
    load_settings,
)
from switch_pipeline.transport.admin import TopicAdmin
from switch_pipeline.transport.producer import KafkaEventPublisher

log = get_logger(__name__)


def source_id_for(table: SourceTable) -> str:
    return f"snowflake:{table.qualified_name}"


def run_adapter(*, once: bool) -> int:
    snowflake = load_settings(SnowflakeSettings)
    source_settings = load_settings(SourceSettings)
    adapter = load_settings(AdapterSettings)
    kafka = load_settings(KafkaSettings)
    postgres = load_settings(PostgresSettings)
    configure_logging(load_settings(LogSettings), service="adapter")

    table = SourceTable.from_settings(snowflake, source_settings)
    source_id = source_id_for(table)
    shutdown = Shutdown().install_signal_handlers()
    heartbeat = Heartbeat(adapter.heartbeat_path)
    conninfo = postgres.conninfo(application_name="switch-adapter")

    TopicAdmin(kafka, client_id="switch-adapter-admin").require_topic()
    store = PostgresSyncStateStore(conninfo)
    store.open(timeout=float(postgres.connect_timeout_seconds))
    source = SnowflakeChangeSource(
        SnowflakeConnectionFactory(snowflake, query_tag="switch-adapter"), table
    )
    publisher = KafkaEventPublisher(kafka, client_id="switch-adapter", sleep=shutdown.sleep)
    try:
        with SourceLock(conninfo, source_id) as lock:
            recovered = store.recover_interrupted_batches(source_id)
            if recovered:
                log.warning("interrupted_batches_closed", count=recovered)
            service = SyncService(
                source_id=source_id,
                source=source,
                state_store=store,
                publisher=publisher,
                mapper=EventMapper(
                    source=SourceRef(system="snowflake", object=table.qualified_name),
                    entity_type=source_settings.entity_type,
                ),
                batch_size=adapter.batch_size,
                settle_seconds=adapter.settle_seconds,
                guard=lock,
                stop_requested=shutdown.requested,
                on_progress=heartbeat.beat,
            )
            log.info(
                "adapter_started",
                source_id=source_id,
                topic=kafka.topic,
                batch_size=adapter.batch_size,
                settle_seconds=adapter.settle_seconds,
                poll_interval_seconds=adapter.poll_interval_seconds,
                run_once=once,
            )
            if once:
                result = service.run_cycle()
                log.info(
                    "sync_cycle_completed",
                    mode=result.mode.value,
                    batches=result.batches,
                    rows=result.rows,
                )
            else:
                service.run_forever(
                    poll_interval_seconds=adapter.poll_interval_seconds,
                    backoff=Backoff(
                        adapter.retry_backoff_initial_seconds, adapter.retry_backoff_max_seconds
                    ),
                    shutdown=shutdown,
                    heartbeat=heartbeat,
                )
    finally:
        publisher.close()
        source.close()
        store.close()
    log.info("adapter_stopped")
    return 0
