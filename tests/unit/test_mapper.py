from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from switch_pipeline.adapter.mapper import EventMapper, to_json_value
from switch_pipeline.adapter.ports import SourceContractError, SourceRow
from switch_pipeline.domain.envelope import EventType
from tests.helpers import SOURCE, T0

MAPPER = EventMapper(source=SOURCE, entity_type="order")


def source_row(*, key: int = 42, version: int = 1, **values: Any) -> SourceRow:
    return SourceRow(
        key=key,
        version=version,
        updated_at=T0,
        values={
            "O_ORDERKEY": key,
            "ROW_VERSION": version,
            "UPDATED_AT": T0.replace(tzinfo=None),
            **values,
        },
    )


def test_version_one_is_an_insert_and_later_versions_are_updates() -> None:
    batch_id = uuid4()
    insert = MAPPER.to_event(source_row(version=1), batch_id=batch_id, captured_at=T0)
    update = MAPPER.to_event(source_row(version=4), batch_id=batch_id, captured_at=T0)
    assert insert.event_type is EventType.INSERT
    assert (update.event_type, update.entity_version) == (EventType.UPDATE, 4)


def test_envelope_carries_source_time_capture_time_and_batch() -> None:
    batch_id = uuid4()
    captured = T0.replace(minute=5)
    event = MAPPER.to_event(source_row(key=9), batch_id=batch_id, captured_at=captured)
    assert (event.entity_key, event.occurred_at, event.captured_at, event.batch_id) == (
        "9",
        T0,
        captured,
        batch_id,
    )
    assert event.payload["o_orderkey"] == 9
    assert event.payload["updated_at"] == "2026-01-01T12:00:00+00:00"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1234.50"), "1234.50"),
        (Decimal("1E+2"), "100"),
        (Decimal("-0.01"), "-0.01"),
        (date(1995, 3, 1), "1995-03-01"),
        (datetime(2026, 1, 1, 8, 30), "2026-01-01T08:30:00+00:00"),
        (time(8, 30), "08:30:00"),
        (b"\x00\x01", "AAE="),
        (float("nan"), "nan"),
        (1.5, 1.5),
        (None, None),
        (True, True),
    ],
)
def test_values_map_to_json_without_losing_precision(value: object, expected: object) -> None:
    assert to_json_value(value) == expected


def test_unsupported_types_stop_the_adapter_instead_of_being_dropped() -> None:
    with pytest.raises(SourceContractError, match="unsupported"):
        to_json_value(object())
