"""Exponential backoff with jitter, shared by every retry loop in the pipeline."""

import random
from dataclasses import dataclass

_JITTER = random.Random()  # noqa: S311 - jitter only spreads retries, it needs no crypto strength
_MAX_EXPONENT = 32


@dataclass(frozen=True, slots=True)
class Backoff:
    initial_seconds: float
    max_seconds: float

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0 or self.max_seconds < self.initial_seconds:
            raise ValueError("backoff needs 0 < initial_seconds <= max_seconds")

    def delay(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Seconds to wait before retry number ``attempt`` (1-based).

        The ceiling doubles per attempt up to ``max_seconds``; the actual delay
        is drawn from [ceiling / 2, ceiling] ("equal jitter") so that many
        clients recovering from the same outage do not retry in lockstep.
        """
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        exponent = min(attempt - 1, _MAX_EXPONENT)
        ceiling = min(self.max_seconds, self.initial_seconds * 2**exponent)
        return (rng or _JITTER).uniform(ceiling / 2, ceiling)
