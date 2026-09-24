"""What the consumer needs from the store it materializes into."""

from collections.abc import Collection, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Protocol
from uuid import UUID

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.domain.log import LogPosition
from switch_pipeline.domain.quarantine import QuarantineEntry


class SinkWriter(Protocol):
    """Writes inside one batch transaction. Every write is idempotent, so
    reprocessing a record (a redelivery, a replay, a rebuild) converges."""

    def known_fingerprints(self, event_ids: Collection[UUID]) -> dict[UUID, str]: ...

    def fingerprint_of(self, event_id: UUID) -> str | None: ...

    def insert_event(
        self,
        event: ChangeEvent,
        *,
        fingerprint: str,
        warnings: Sequence[Mapping[str, str]],
        position: LogPosition,
    ) -> bool:
        """Append to the event log; False if the event id is already logged."""
        ...

    def upsert_current_state(self, event: ChangeEvent) -> bool:
        """Move the entity to this version unless an equal or newer one is current."""
        ...

    def quarantine(self, entry: QuarantineEntry) -> bool:
        """Record a rejected record; False if it was already quarantined."""
        ...

    def increment_counter(self, name: str, amount: int) -> None: ...

    def atomic(self) -> AbstractContextManager[None]:
        """All or nothing for one record's writes.

        Raises RecordRejectedError when the sink cannot store a value; the
        batch transaction stays usable for the remaining records.
        """
        ...


class Sink(Protocol):
    def transaction(self) -> AbstractContextManager[SinkWriter]:
        """One transaction, committed when the block exits cleanly.

        Raises DatabaseUnavailableError when the database cannot be reached.
        """
        ...
