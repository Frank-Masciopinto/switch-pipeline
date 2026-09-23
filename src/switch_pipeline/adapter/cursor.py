"""The sync watermark: a composite cursor over (updated_at, key). Pure logic, no I/O.

A timestamp alone is not a safe watermark: many rows share one timestamp (a
bulk load stamps them all at once), so ``updated_at > last`` would skip the
rows of a tie that did not fit in the previous batch. Ordering by the pair and
resuming strictly after the last pair read is exact, even across restarts.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from switch_pipeline.errors import FatalPipelineError


class CursorRegressionError(FatalPipelineError):
    """The source returned rows that do not advance the watermark."""


@dataclass(frozen=True, slots=True)
class SyncCursor:
    updated_at: datetime
    key: int | str

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError("cursor timestamp must be timezone-aware")
        if isinstance(self.key, bool) or not isinstance(self.key, int | str):
            raise TypeError(f"cursor key must be int or str, got {type(self.key).__name__}")
        object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))

    def is_after(self, other: "SyncCursor") -> bool:
        if type(self.key) is not type(other.key):
            raise CursorRegressionError(f"cursor key type changed: {other.key!r} -> {self.key!r}")
        return (self.updated_at, self.key) > (other.updated_at, other.key)

    def to_json(self) -> dict[str, str | int]:
        return {"updated_at": self.updated_at.isoformat(), "key": self.key}

    @classmethod
    def from_json(cls, data: Mapping[str, object]) -> "SyncCursor":
        updated_at, key = data["updated_at"], data["key"]
        if not isinstance(updated_at, str) or not isinstance(key, int | str):
            raise TypeError(f"malformed cursor: {dict(data)!r}")
        return cls(updated_at=datetime.fromisoformat(updated_at), key=key)


def advance_cursor(current: SyncCursor | None, positions: Sequence[SyncCursor]) -> SyncCursor:
    """Check a fetched batch continues strictly after ``current`` in ascending
    order and return the watermark after it.

    The query guarantees this; the check turns a broken assumption (wrong
    column type, collation surprise, a writer rewinding timestamps) into a loud
    failure before anything is published, instead of silent loss or re-emission.
    """
    if not positions:
        raise ValueError("cannot advance over an empty batch")
    previous = current
    for position in positions:
        if previous is not None and not position.is_after(previous):
            raise CursorRegressionError(
                f"row {position.to_json()} does not come strictly after {previous.to_json()}"
            )
        previous = position
    return positions[-1]
