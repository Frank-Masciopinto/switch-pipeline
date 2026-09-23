"""Snowflake (emulated) -> adapter -> Redpanda -> consumer -> PostgreSQL, all real code."""

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from switch_pipeline.adapter.mapper import EventMapper
from switch_pipeline.adapter.publisher import KafkaEventPublisher
from switch_pipeline.adapter.service import SyncService
from switch_pipeline.adapter.source import (
    SnowflakeChangeSource,
    SnowflakeConnectionFactory,
    SourceTable,
)
from switch_pipeline.adapter.state import PostgresSyncStateStore
from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.consumer.runner import ConsumerRunner
from switch_pipeline.db.queries import SINK_CHECKSUMS, TRUNCATE_SINK
from switch_pipeline.domain.envelope import SourceRef
from switch_pipeline.kafka import TopicAdmin
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ConsumerSettings, KafkaSettings, SnowflakeSettings
from switch_pipeline.tools.seed import seed_source
from switch_pipeline.tools.simulate import simulate_changes
from tests.integration.conftest import RULES_PATH, SOURCE_SETTINGS, synthetic_seed


class Pipeline:
    def __init__(
        self,
        snowflake: SnowflakeSettings,
        kafka: KafkaSettings,
        consumer: ConsumerSettings,
        db: str,
        pool: ConnectionPool,
    ) -> None:
        self.db = db
        table = SourceTable.from_settings(snowflake, SOURCE_SETTINGS)
        self.source = SnowflakeChangeSource(
            SnowflakeConnectionFactory(snowflake, query_tag="tests"), table
        )
        self.store = PostgresSyncStateStore(db)
        self.store.open(timeout=10)
        self.publisher = KafkaEventPublisher(kafka, client_id="tests", sleep=Shutdown().sleep)
        self.mapper = EventMapper(
            source=SourceRef(system="snowflake", object=table.qualified_name), entity_type="order"
        )
        self.source_id = f"snowflake:{table.qualified_name}"
        self.runner = ConsumerRunner(
            kafka=kafka,
            settings=consumer,
            pool=pool,
            processor=EventProcessor(load_rules(RULES_PATH)),
            shutdown=Shutdown(),
            heartbeat=Heartbeat(None),
            client_id="tests",
        )

    def adapter(self) -> SyncService:
        """A fresh adapter instance over the same durable state (i.e. after a restart)."""
        return SyncService(
            source_id=self.source_id,
            source=self.source,
            state_store=self.store,
            publisher=self.publisher,
            mapper=self.mapper,
            batch_size=100,
            settle_seconds=0,
        )

    def consume(self, *, from_beginning: bool = False) -> dict[Outcome, int]:
        report = self.runner.catch_up(from_beginning=from_beginning)
        assert report.complete
        return dict(report.outcomes)

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with psycopg.connect(self.db) as conn:
            return conn.cursor(row_factory=dict_row).execute(sql, params).fetchall()  # type: ignore[arg-type]

    def close(self) -> None:
        self.publisher.close()
        self.source.close()
        self.store.close()


@pytest.fixture
def pipeline(
    snowflake_settings: SnowflakeSettings,
    kafka_settings: KafkaSettings,
    consumer_settings: ConsumerSettings,
    db: str,
    sink_pool: ConnectionPool,
) -> Iterator[Pipeline]:
    TopicAdmin(kafka_settings, client_id="tests").ensure_topic()
    seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(300), force=False)
    running = Pipeline(snowflake_settings, kafka_settings, consumer_settings, db, sink_pool)
    yield running
    running.close()


def test_initial_sync_incremental_changes_quarantine_and_replay_convergence(
    pipeline: Pipeline, snowflake_settings: SnowflakeSettings
) -> None:
    adapter = pipeline.adapter()
    assert adapter.run_cycle().rows == 300
    assert adapter.run_cycle().rows == 0, "a rerun must not re-emit synced rows"
    assert pipeline.consume() == {Outcome.APPLIED: 300}

    changes = simulate_changes(
        snowflake_settings, SOURCE_SETTINGS, inserts=5, updates=7, invalid_rows=3
    )
    restarted = pipeline.adapter()
    assert restarted.run_cycle().rows == 15
    assert pipeline.consume() == {Outcome.APPLIED: 12, Outcome.QUARANTINED: 3}

    state = {
        row["entity_key"]: row
        for row in pipeline.query("SELECT entity_key, entity_version FROM entity_current_state")
    }
    assert len(state) == 305
    assert all(state[str(key)]["entity_version"] == 2 for key in changes.updated)
    rejected = {str(item["key"]): item["kind"] for item in changes.invalid}
    status_key = next(key for key, kind in rejected.items() if kind == "unknown_order_status")
    assert state[status_key]["entity_version"] == 1, "state keeps the last good version"
    assert {
        row["entity_key"] for row in pipeline.query("SELECT entity_key FROM quarantine")
    } == set(rejected)

    [watermark] = pipeline.query("SELECT cursor_key, initial_sync_completed_at FROM sync_state")
    assert watermark["initial_sync_completed_at"] is not None

    before = pipeline.query(SINK_CHECKSUMS)[0]
    with psycopg.connect(pipeline.db, autocommit=True) as conn:
        conn.execute(TRUNCATE_SINK)
    assert pipeline.consume(from_beginning=True) == {
        Outcome.APPLIED: 312,
        Outcome.QUARANTINED: 3,
    }
    assert pipeline.query(SINK_CHECKSUMS)[0] == before
