"""In-memory implementations of the adapter's ports, for fast unit tests.

tests/integration/test_port_contracts.py runs the same scenarios against these
and against the real Snowflake source and PostgreSQL store, so a fake cannot
quietly drift from the behaviour it stands in for.
"""

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.adapter.ports import SourceRow, SyncMode, SyncState
from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import BrokerUnavailableError, FatalPipelineError
from tests.helpers import T0


class FakeSource:
    """A ChangeSource over rows held in memory, with a clock the test controls."""

    def __init__(self) -> None:
        self.now = T0
        self.rows: dict[int, SourceRow] = {}

    def write(self, key: int, *, at: datetime | None = None, **values: Any) -> None:
        previous = self.rows.get(key)
        version = previous.version + 1 if previous else 1
        self.rows[key] = SourceRow(
            key=key,
            version=version,
            updated_at=at or self.now,
            values={"O_ORDERKEY": key, "ROW_VERSION": version, **values},
        )

    def upper_bound(self, settle_seconds: int) -> datetime:
        return self.now - timedelta(seconds=settle_seconds)

    def fetch_changes(
        self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
    ) -> list[SourceRow]:
        ordered = sorted(self.rows.values(), key=lambda row: (row.updated_at, row.key))
        after = [
            row
            for row in ordered
            if row.updated_at <= upper_bound and (cursor is None or row.position.is_after(cursor))
        ]
        return after[:limit]


@dataclass
class _Batch:
    source_id: str
    status: str


class MemoryStateStore:
    """A SyncStateStore in memory."""

    def __init__(self) -> None:
        self._states: dict[str, SyncState] = {}
        self._batches: dict[UUID, _Batch] = {}

    def load(self, source_id: str) -> SyncState:
        return self._states.setdefault(
            source_id,
            SyncState(
                source_id=source_id,
                cursor=None,
                initial_sync_completed_at=None,
                last_batch_id=None,
            ),
        )

    def begin_batch(
        self,
        *,
        batch_id: UUID,
        source_id: str,
        mode: SyncMode,
        cursor_start: SyncCursor | None,
        upper_bound: datetime,
    ) -> None:
        self._batches[batch_id] = _Batch(source_id=source_id, status="running")

    def commit_batch(
        self, *, batch_id: UUID, source_id: str, cursor_end: SyncCursor, row_count: int
    ) -> None:
        batch = self._batches.get(batch_id)
        if batch is None or batch.status != "running":
            raise FatalPipelineError(f"batch {batch_id} is not running; refusing to commit it")
        batch.status = "committed"
        self._states[source_id] = replace(
            self.load(source_id), cursor=cursor_end, last_batch_id=batch_id
        )

    def fail_batch(self, *, batch_id: UUID, error: str) -> None:
        batch = self._batches.get(batch_id)
        if batch is not None and batch.status == "running":
            batch.status = "failed"

    def mark_initial_sync_completed(self, source_id: str) -> None:
        state = self.load(source_id)
        if state.initial_sync_completed_at is None:
            self._states[source_id] = replace(state, initial_sync_completed_at=datetime.now(UTC))

    def statuses(self) -> list[str]:
        """Every batch's status, in the order the batches began."""
        return [batch.status for batch in self._batches.values()]


class BrokerLog:
    """What the topic holds. Can fail after part of a batch was already written."""

    def __init__(self) -> None:
        self.events: list[ChangeEvent] = []
        self.failures_to_inject = 0

    def publish(self, events: Sequence[ChangeEvent]) -> None:
        if self.failures_to_inject:
            self.failures_to_inject -= 1
            self.events.extend(events[: len(events) // 2])
            raise BrokerUnavailableError("broker unavailable")
        self.events.extend(events)

    def keys(self) -> list[str]:
        return [event.entity_key for event in self.events]
