import random

import pytest

from switch_pipeline.retry import Backoff


@pytest.mark.parametrize(
    ("attempt", "ceiling"), [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 10.0), (500, 10.0)]
)
def test_delay_doubles_with_jitter_and_is_capped(attempt: int, ceiling: float) -> None:
    backoff = Backoff(initial_seconds=1.0, max_seconds=10.0)
    rng = random.Random(attempt)
    for _ in range(50):
        assert ceiling / 2 <= backoff.delay(attempt, rng=rng) <= ceiling


def test_jitter_spreads_concurrent_retries() -> None:
    backoff = Backoff(initial_seconds=1.0, max_seconds=60.0)
    rng = random.Random(0)
    assert len({round(backoff.delay(5, rng=rng), 6) for _ in range(20)}) > 1


@pytest.mark.parametrize(("initial", "maximum"), [(0.0, 1.0), (2.0, 1.0), (-1.0, 5.0)])
def test_rejects_nonsensical_settings(initial: float, maximum: float) -> None:
    with pytest.raises(ValueError, match="backoff"):
        Backoff(initial_seconds=initial, max_seconds=maximum)


def test_attempts_are_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        Backoff(initial_seconds=1.0, max_seconds=2.0).delay(0)
