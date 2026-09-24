"""What the sync service needs from the outside world.

The service sees only these interfaces. Snowflake (adapter/snowflake.py),
PostgreSQL (sink/sync_state.py) and Kafka (transport/producer.py) implement
them, and each reports failures with the error types of errors.py.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import FatalPipelineError


class SourceContractError(FatalPipelineError):
    """A row violates the source contract (e.g. a NULL key or an unsupported type)."""


@dataclass(frozen=True, slots=True)
class SourceRow:
    key: int | str
    version: int
    updated_at: datetime  # timezone-aware UTC
    values: Mapping[str, Any]  # the full row, column name -> Python value

    @property
    def position(self) -> SyncCursor:
        return SyncCursor(updated_at=self.updated_at, key=self.key)


class ChangeSource(Protocol):
    """Raises SourceUnavailableError while the source cannot be read."""

    def upper_bound(self, settle_seconds: int) -> datetime:
        """The newest ``updated_at`` this cycle may read (source clock, UTC)."""
        ...

    def fetch_changes(
        self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
    ) -> list[SourceRow]:
        """Rows strictly after ``cursor`` and at or before ``upper_bound``, in watermark order."""
        ...


class SyncMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True, slots=True, kw_only=True)
class SyncState:
    source_id: str
    cursor: SyncCursor | None
    initial_sync_completed_at: datetime | None
    last_batch_id: UUID | None

    @property
    def mode(self) -> SyncMode:
        return SyncMode.FULL if self.initial_sync_completed_at is None else SyncMode.INCREMENTAL


class SyncStateStore(Protocol):
    """The durable watermark and batch ledger.

    Raises DatabaseUnavailableError when its database cannot be reached.
    """

    def load(self, source_id: str) -> SyncState: ...

    def begin_batch(
        self,
        *,
        batch_id: UUID,
        source_id: str,
        mode: SyncMode,
        cursor_start: SyncCursor | None,
        upper_bound: datetime,
    ) -> None: ...

    def commit_batch(
        self, *, batch_id: UUID, source_id: str, cursor_end: SyncCursor, row_count: int
    ) -> None:
        """Mark a running batch committed and move the watermark, atomically.

        Raises FatalPipelineError if the batch is not running.
        """
        ...

    def fail_batch(self, *, batch_id: UUID, error: str) -> None: ...

    def mark_initial_sync_completed(self, source_id: str) -> None: ...


class EventPublisher(Protocol):
    def publish(self, events: Sequence[ChangeEvent]) -> None:
        """Return once every event is durably accepted, else raise.

        BrokerUnavailableError means some events may not have been delivered
        and the batch can be published again.
        """
        ...


class OwnershipGuard(Protocol):
    def check(self) -> None:
        """Raise FatalPipelineError if this process no longer owns the source."""
        ...
