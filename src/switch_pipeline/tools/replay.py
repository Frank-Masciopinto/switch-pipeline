"""Replays the topic from offset 0 and verifies the sink converges to the same state.

Both states are taken over exactly the same records: the end offsets are
snapshotted once, the group first catches up to them, the "before" checksums
are taken, then the replay runs from offset 0 up to the same offsets.
"""

from dataclasses import asdict, dataclass
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from switch_pipeline.consumer.main import open_sink_pool
from switch_pipeline.consumer.processor import EventProcessor
from switch_pipeline.consumer.runner import ConsumerRunner
from switch_pipeline.db.queries import SINK_CHECKSUMS, TRUNCATE_SINK
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.kafka import TopicAdmin
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import get_logger
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ConsumerSettings, KafkaSettings, PostgresSettings

log = get_logger(__name__)

_COMPARED = ("state_checksum", "event_log_checksum", "quarantine_checksum")


@dataclass(frozen=True, slots=True)
class ReplayReport:
    mode: str
    records_replayed: int
    outcomes: dict[str, int]
    before: dict[str, Any]
    after: dict[str, Any]
    converged: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def replay_topic(
    kafka: KafkaSettings,
    consumer: ConsumerSettings,
    postgres: PostgresSettings,
    *,
    rebuild: bool,
    force: bool,
) -> ReplayReport:
    members = TopicAdmin(kafka, client_id="switch-replay-admin").active_members(
        kafka.consumer_group
    )
    if members and not force:
        raise FatalPipelineError(
            f"consumer group {kafka.consumer_group!r} has {members} active member(s); "
            "stop the consumer first (`make replay` does this)"
        )
    pool = open_sink_pool(postgres, application_name="switch-replay")
    try:
        runner = ConsumerRunner(
            kafka=kafka,
            settings=consumer,
            pool=pool,
            processor=EventProcessor(load_rules(consumer.quality_rules_path)),
            shutdown=Shutdown().install_signal_handlers(),
            heartbeat=Heartbeat(None),
            client_id="switch-replay",
        )
        bounds = runner.end_offsets()
        runner.catch_up(from_beginning=False, until=bounds)
        before = _checksums(pool)
        if rebuild:
            with pool.connection() as conn:
                conn.execute(TRUNCATE_SINK)
            log.info("sink_truncated")
        report = runner.catch_up(from_beginning=True, until=bounds)
        after = _checksums(pool)
    finally:
        pool.close()
    converged = report.complete and all(before[name] == after[name] for name in _COMPARED)
    return ReplayReport(
        mode="rebuild" if rebuild else "reprocess",
        records_replayed=sum(report.outcomes.values()),
        outcomes={outcome.value: count for outcome, count in sorted(report.outcomes.items())},
        before=before,
        after=after,
        converged=converged,
    )


def _checksums(pool: ConnectionPool) -> dict[str, Any]:
    with pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(SINK_CHECKSUMS).fetchone()
    assert row is not None
    return dict(row)
