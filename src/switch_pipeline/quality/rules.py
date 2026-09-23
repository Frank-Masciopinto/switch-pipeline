"""Declarative data-quality rules, loaded from the YAML file at QUALITY_RULES_PATH.

Adding a rule for an existing check is a YAML edit; adding a new *kind* of
check means one new model class below plus its entry in ``Rule``.

Every check is a pure function of the event (it never looks at the wall clock
or the database), so replaying the topic always reaches the same verdicts.
Null or missing values pass every check except ``not_null``.
"""

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from switch_pipeline.domain.envelope import ENTITY_TYPE_PATTERN, ChangeEvent


class Severity(StrEnum):
    REJECT = "reject"  # event is quarantined; current state keeps the last good version
    WARN = "warn"  # event is accepted; the violation is recorded on its event-log row


@dataclass(frozen=True, slots=True)
class Violation:
    rule: str
    field: str
    severity: Severity
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "rule": self.rule,
            "field": self.field,
            "severity": self.severity.value,
            "message": self.message,
        }


class _Rule(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    description: str = Field(min_length=1)
    field: str = Field(min_length=1)
    severity: Severity

    def evaluate(self, event: ChangeEvent) -> Violation | None:
        present = self.field in event.payload
        message = self._check(event.payload.get(self.field), present=present, event=event)
        if message is None:
            return None
        return Violation(rule=self.name, field=self.field, severity=self.severity, message=message)

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        raise NotImplementedError


class NotNullRule(_Rule):
    check: Literal["not_null"]

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if not present:
            return "field is missing"
        return "value is null" if value is None else None


class MinRule(_Rule):
    check: Literal["min"]
    value: Decimal

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None:
            return None
        number = _as_decimal(value)
        if number is None:
            return f"value {value!r} is not numeric"
        return f"value {number} is below the minimum {self.value}" if number < self.value else None


class MaxRule(_Rule):
    check: Literal["max"]
    value: Decimal

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None:
            return None
        number = _as_decimal(value)
        if number is None:
            return f"value {value!r} is not numeric"
        return f"value {number} is above the maximum {self.value}" if number > self.value else None


class AllowedValuesRule(_Rule):
    check: Literal["allowed_values"]
    values: tuple[str | int | bool, ...] = Field(min_length=1)

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None or value in self.values:
            return None
        return f"value {value!r} is not one of {list(self.values)}"


class PatternRule(_Rule):
    check: Literal["pattern"]
    regex: str

    @field_validator("regex")
    @classmethod
    def _compiles(cls, regex: str) -> str:
        re.compile(regex)
        return regex

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return f"value {value!r} is not a string"
        return (
            None
            if re.fullmatch(self.regex, value)
            else f"value {value!r} does not match {self.regex!r}"
        )


class MaxLengthRule(_Rule):
    check: Literal["max_length"]
    value: int = Field(ge=0)

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return f"value {value!r} is not a string"
        if len(value) > self.value:
            return f"length {len(value)} exceeds {self.value}"
        return None


class MatchesEntityKeyRule(_Rule):
    check: Literal["matches_entity_key"]

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None or str(value) == event.entity_key:
            return None
        return f"value {value!r} does not match entity_key {event.entity_key!r}"


class NotAfterCapturedAtRule(_Rule):
    check: Literal["not_after_captured_at"]

    def _check(self, value: JsonValue, *, present: bool, event: ChangeEvent) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return f"value {value!r} is not an ISO-8601 date or timestamp"
        try:
            moment = _parse_iso(value)
        except ValueError:
            return f"value {value!r} is not an ISO-8601 date or timestamp"
        captured = event.captured_at
        is_later = moment > captured if isinstance(moment, datetime) else moment > captured.date()
        return (
            f"{value} is later than the capture time {captured.isoformat()}" if is_later else None
        )


Rule = Annotated[
    NotNullRule
    | MinRule
    | MaxRule
    | AllowedValuesRule
    | PatternRule
    | MaxLengthRule
    | MatchesEntityKeyRule
    | NotAfterCapturedAtRule,
    Field(discriminator="check"),
]


@dataclass(frozen=True, slots=True)
class QualityReport:
    violations: tuple[Violation, ...]

    @property
    def rejections(self) -> tuple[Violation, ...]:
        return tuple(v for v in self.violations if v.severity is Severity.REJECT)

    @property
    def warnings(self) -> tuple[Violation, ...]:
        return tuple(v for v in self.violations if v.severity is Severity.WARN)


class RuleSet(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1]
    entities: dict[Annotated[str, Field(pattern=ENTITY_TYPE_PATTERN)], tuple[Rule, ...]]

    @model_validator(mode="after")
    def _unique_names(self) -> Self:
        for entity_type, rules in self.entities.items():
            names = [rule.name for rule in rules]
            duplicates = sorted({name for name in names if names.count(name) > 1})
            if duplicates:
                raise ValueError(f"duplicate rule names for {entity_type!r}: {duplicates}")
        return self

    def evaluate(self, event: ChangeEvent) -> QualityReport:
        rules = self.entities.get(event.entity_type, ())
        return QualityReport(tuple(v for rule in rules if (v := rule.evaluate(event)) is not None))


@dataclass(frozen=True, slots=True)
class LoadedRuleSet:
    rules: RuleSet
    fingerprint: str  # changes only when rule semantics change, not on comment edits
    path: Path

    def evaluate(self, event: ChangeEvent) -> QualityReport:
        return self.rules.evaluate(event)

    def summary(self) -> dict[str, int]:
        return {entity: len(rules) for entity, rules in self.rules.entities.items()}


def load_rules(path: Path) -> LoadedRuleSet:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    rules = RuleSet.model_validate(raw)
    fingerprint = hashlib.sha256(rules.model_dump_json().encode("utf-8")).hexdigest()[:16]
    return LoadedRuleSet(rules=rules, fingerprint=fingerprint, path=path)


def _as_decimal(value: JsonValue) -> Decimal | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float | str):
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return None
        return number if number.is_finite() else None
    return None


def _parse_iso(value: str) -> date | datetime:
    if "T" in value or " " in value:
        moment = datetime.fromisoformat(value)
        return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
    return date.fromisoformat(value)
