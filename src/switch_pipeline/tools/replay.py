"""Replays the topic from offset 0 and verifies the sink converges to the same state.

Both states are taken over exactly the same records: the end offsets are
snapshotted once, the group first catches up to them, the "before" checksums
are taken, then the replay runs from offset 0 up to the same offsets.
"""

import time
from contextlib import closing
from dataclasses import asdict, dataclass
from typing import Any

from switch_pipeline.consumer.processor import EventProcessor
from switch_pipeline.consumer.runner import ConsumerRunner
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import get_logger
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ConsumerSettings, KafkaSettings, PostgresSettings
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.transport.admin import TopicAdmin

log = get_logger(__name__)

_COMPARED = ("state_checksum", "event_log_checksum", "quarantine_checksum")

# Longer than librdkafka's default session.timeout.ms (45 s), after which the
# broker evicts a member that disappeared without leaving the group.
_GROUP_EVICTION_SECONDS = 60.0


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
    shutdown: Shutdown,
) -> ReplayReport:
    if not force:
        _wait_for_idle_group(TopicAdmin(kafka, client_id="switch-replay-admin"), kafka)
    with closing(PostgresSink.open(postgres, application_name="switch-replay")) as sink:
        runner = ConsumerRunner(
            kafka=kafka,
            settings=consumer,
            sink=sink,
            processor=EventProcessor(load_rules(consumer.quality_rules_path)),
            shutdown=shutdown,
            heartbeat=Heartbeat(None),
            client_id="switch-replay",
        )
        bounds = runner.end_offsets()
        runner.catch_up(from_beginning=False, until=bounds)
        before = sink.checksums()
        if rebuild:
            sink.truncate()
            log.info("sink_truncated")
        report = runner.catch_up(from_beginning=True, until=bounds)
        after = sink.checksums()
    converged = report.complete and all(before[name] == after[name] for name in _COMPARED)
    return ReplayReport(
        mode="rebuild" if rebuild else "reprocess",
        records_replayed=sum(report.outcomes.values()),
        outcomes={outcome.value: count for outcome, count in sorted(report.outcomes.items())},
        before=before,
        after=after,
        converged=converged,
    )


def _wait_for_idle_group(admin: TopicAdmin, kafka: KafkaSettings) -> None:
    """Replay assigns partitions manually, which is only safe with no live member.

    A consumer stopped mid-join may stay listed until the broker evicts it.
    """
    deadline = time.monotonic() + _GROUP_EVICTION_SECONDS
    while members := admin.active_members(kafka.consumer_group):
        if time.monotonic() >= deadline:
            raise FatalPipelineError(
                f"consumer group {kafka.consumer_group!r} still has {members} active member(s); "
                "stop the consumer first (`make replay` does this)"
            )
        log.info("waiting_for_idle_consumer_group", group=kafka.consumer_group, members=members)
        time.sleep(2)
