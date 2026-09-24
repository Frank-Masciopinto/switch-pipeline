from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import JsonValue, ValidationError

from switch_pipeline.errors import ConfigurationError
from switch_pipeline.quality.rules import RuleSet, Severity, load_rules
from tests.helpers import REPO_ROOT, T0, make_event, order_payload, rule

MISSING = object()


def ruleset(*rules: dict[str, Any], entity: str = "order") -> RuleSet:
    return RuleSet.model_validate({"version": 1, "entities": {entity: list(rules)}})


def verdict(definition: dict[str, Any], value: object) -> bool:
    payload: dict[str, JsonValue] = {} if value is MISSING else {"f": value}  # type: ignore[dict-item]
    return not ruleset(definition).evaluate(make_event(payload=payload)).violations


@pytest.mark.parametrize(
    ("definition", "value", "passes"),
    [
        (rule("not_null"), 0, True),
        (rule("not_null"), None, False),
        (rule("not_null"), MISSING, False),
        (rule("min", value=0), "12.50", True),
        (rule("min", value=0), 0, True),
        (rule("min", value=0), "-0.01", False),
        (rule("min", value=0), "not a number", False),
        (rule("min", value=0), True, False),
        (rule("min", value=0), None, True),
        (rule("max", value=10), 10, True),
        (rule("max", value=10), 10.5, False),
        (rule("allowed_values", values=["O", "F"]), "F", True),
        (rule("allowed_values", values=["O", "F"]), "X", False),
        (rule("allowed_values", values=["O", "F"]), None, True),
        (rule("pattern", regex=r"Clerk#\d{9}"), "Clerk#000000042", True),
        (rule("pattern", regex=r"Clerk#\d{9}"), "Clerk#42", False),
        (rule("pattern", regex=r"Clerk#\d{9}"), 42, False),
        (rule("max_length", value=3), "abc", True),
        (rule("max_length", value=3), "abcd", False),
    ],
)
def test_checks(definition: dict[str, Any], value: object, passes: bool) -> None:
    assert verdict(definition, value) is passes


def test_matches_entity_key_compares_with_the_envelope_key() -> None:
    rules = ruleset(rule("matches_entity_key", field="o_orderkey"))
    assert not rules.evaluate(make_event(key=42, payload=order_payload(42))).violations
    assert rules.evaluate(make_event(key=42, payload=order_payload(43))).violations


@pytest.mark.parametrize(
    ("value", "passes"),
    [
        (T0.date().isoformat(), True),
        ("1996-01-01", True),
        ("2099-01-01", False),
        ("2026-01-01T12:00:05+00:00", True),
        ("2026-01-01T13:00:00+00:00", False),
        ("yesterday", False),
    ],
)
def test_not_after_captured_at_is_relative_to_capture_not_wall_clock(
    value: str, passes: bool
) -> None:
    rules = ruleset(rule("not_after_captured_at", field="d"))
    event = make_event(payload={"d": value}, occurred_at=T0, captured_at=T0 + timedelta(seconds=10))
    assert (not rules.evaluate(event).violations) is passes


def test_warn_rules_record_without_rejecting() -> None:
    rules = ruleset(rule("max_length", value=1, severity="warn"), rule("not_null", field="g"))
    report = rules.evaluate(make_event(payload={"f": "long", "g": 1}))
    assert [v.severity for v in report.warnings] == [Severity.WARN]
    assert report.rejections == ()


def test_rules_only_apply_to_their_entity_type() -> None:
    rules = ruleset(rule("not_null", field="missing"), entity="order")
    assert not rules.evaluate(make_event(entity_type="customer", payload={})).violations


@pytest.mark.parametrize(
    "definitions",
    [
        [rule("no_such_check")],
        [rule("min")],
        [rule("pattern", regex="([unclosed")],
        [rule("not_null"), rule("not_null")],
        [{**rule("not_null"), "unknown_option": True}],
    ],
    ids=["unknown-check", "missing-parameter", "bad-regex", "duplicate-name", "unknown-option"],
)
def test_invalid_rule_files_fail_at_load(definitions: list[dict[str, Any]]) -> None:
    with pytest.raises(ValidationError):
        ruleset(*definitions)


def test_shipped_rules_file_is_valid_and_rejects_bad_orders() -> None:
    loaded = load_rules(REPO_ROOT / "config" / "quality_rules.yaml")
    order_rules = loaded.rules.entities["order"]
    assert sum(r.severity is Severity.REJECT for r in order_rules) >= 3
    good = make_event(key=5, payload=order_payload(5))
    assert not loaded.evaluate(good).violations
    bad = make_event(key=5, payload=order_payload(5, o_totalprice="-42.00", o_orderstatus="X"))
    assert {v.rule for v in loaded.evaluate(bad).rejections} == {
        "total_price_non_negative",
        "order_status_known",
    }


@pytest.mark.parametrize(
    ("content", "problem"),
    [
        (
            "version: 1\nentities:\n  order:\n"
            "  - {name: r, description: d, field: f, severity: reject, check: nope}\n",
            r"invalid quality rules in .*\n  - entities\.order\.0",
        ),
        ("version: 1\nentities: [not closed\n", "is not valid YAML"),
    ],
    ids=["unknown-check", "broken-yaml"],
)
def test_a_broken_rules_file_is_a_configuration_error_naming_the_file(
    tmp_path: Path, content: str, problem: str
) -> None:
    path = tmp_path / "rules.yaml"
    path.write_text(content)
    with pytest.raises(ConfigurationError, match=problem) as caught:
        load_rules(path)
    assert str(path) in str(caught.value)


def test_a_missing_rules_file_is_a_configuration_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="cannot read the quality rules file"):
        load_rules(tmp_path / "absent.yaml")


def test_fingerprint_changes_with_semantics_not_formatting(tmp_path: Path) -> None:
    compact = tmp_path / "a.yaml"
    compact.write_text(
        "version: 1\nentities:\n  order:\n"
        "  - {name: r, description: d, field: f, severity: reject, check: not_null}\n"
    )
    commented = tmp_path / "b.yaml"
    commented.write_text(
        "# comment\nversion: 1\n\nentities:\n  order:\n    - name: r   # why\n"
        "      description: d\n      field: f\n      severity: reject\n      check: not_null\n"
    )
    changed = tmp_path / "c.yaml"
    changed.write_text(commented.read_text().replace("reject", "warn"))
    assert load_rules(compact).fingerprint == load_rules(commented).fingerprint
    assert load_rules(changed).fingerprint != load_rules(commented).fingerprint
