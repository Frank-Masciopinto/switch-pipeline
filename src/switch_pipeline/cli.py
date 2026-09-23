"""Command-line entry point for every service and operator tool."""

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from switch_pipeline import __version__
from switch_pipeline.adapter.main import run_adapter
from switch_pipeline.api.main import run_api
from switch_pipeline.consumer.main import run_consumer
from switch_pipeline.db.migrate import apply_migrations
from switch_pipeline.domain.envelope import envelope_json_schema
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.kafka import TopicAdmin
from switch_pipeline.observability import configure_logging, get_logger
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import (
    AdapterSettings,
    ApiSettings,
    ConfigurationError,
    ConsumerSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    SeedSettings,
    SimulateSettings,
    SnowflakeSettings,
    SourceSettings,
    load_settings,
)
from switch_pipeline.tools.inject import inject_bad_events
from switch_pipeline.tools.replay import replay_topic
from switch_pipeline.tools.seed import seed_source
from switch_pipeline.tools.simulate import simulate_changes

log = get_logger(__name__)

DEFAULT_SCHEMA_PATH = Path("schemas/change_event.v1.schema.json")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = args.handler
    try:
        return handler(args)
    except ConfigurationError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except FatalPipelineError:
        log.exception("fatal_error")
        return 1
    except KeyboardInterrupt:
        return 130


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="switch-pipeline", description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    def command(name: str, handler: Callable[[argparse.Namespace], int], help_text: str) -> Any:
        sub = commands.add_parser(name, help=help_text, description=help_text)
        sub.set_defaults(handler=handler)
        return sub

    command("init", _init, "Apply database migrations and create the Kafka topic (idempotent).")
    command("migrate", _migrate, "Apply database migrations only.")
    adapter = command("adapter", _adapter, "Run the Snowflake -> Kafka adapter.")
    adapter.add_argument("--once", action="store_true", help="Run one sync cycle and exit.")
    command("consumer", _consumer, "Run the Kafka -> PostgreSQL consumer.")
    command("api", _api, "Run the event-inspection API.")
    seed = command("seed", _seed, "Create and fill the Snowflake source table.")
    seed.add_argument(
        "--force", action="store_true", help="Recreate the table even if it has rows."
    )
    simulate = command("simulate", _simulate, "Insert/update rows in the source table.")
    simulate.add_argument("--inserts", type=int, help="Override SIMULATE_INSERTS.")
    simulate.add_argument("--updates", type=int, help="Override SIMULATE_UPDATES.")
    simulate.add_argument("--invalid", type=int, help="Override SIMULATE_INVALID_ROWS.")
    command("inject-bad-events", _inject, "Publish malformed/duplicate records to the topic.")
    replay = command("replay", _replay, "Replay the topic from offset 0 and verify convergence.")
    replay.add_argument(
        "--rebuild",
        action="store_true",
        help="Truncate the sink first and rebuild it from the log.",
    )
    replay.add_argument("--force", action="store_true", help="Skip the idle-consumer-group check.")
    schema = command("export-schema", _export_schema, "Write the envelope JSON Schema.")
    schema.add_argument("--output", type=Path, default=DEFAULT_SCHEMA_PATH)
    command("check-config", _check_config, "Validate .env and the quality rules file.")
    return parser


def _tool_logging(service: str) -> None:
    configure_logging(load_settings(LogSettings), service=service, stream=sys.stderr)


def _report(payload: object) -> int:
    print(json.dumps(payload, indent=2, default=str))
    return 0


def _init(_: argparse.Namespace) -> int:
    kafka = load_settings(KafkaSettings)
    postgres = load_settings(PostgresSettings)
    configure_logging(load_settings(LogSettings), service="init")
    applied = apply_migrations(postgres.conninfo(application_name="switch-init"))
    TopicAdmin(kafka, client_id="switch-init").ensure_topic()
    log.info("init_completed", migrations_applied=applied)
    return 0


def _migrate(_: argparse.Namespace) -> int:
    postgres = load_settings(PostgresSettings)
    configure_logging(load_settings(LogSettings), service="migrate")
    applied = apply_migrations(postgres.conninfo(application_name="switch-migrate"))
    log.info("migrations_completed", applied=applied)
    return 0


def _adapter(args: argparse.Namespace) -> int:
    return run_adapter(once=args.once)


def _consumer(_: argparse.Namespace) -> int:
    return run_consumer()


def _api(_: argparse.Namespace) -> int:
    return run_api()


def _seed(args: argparse.Namespace) -> int:
    snowflake, source = load_settings(SnowflakeSettings), load_settings(SourceSettings)
    seed = load_settings(SeedSettings)
    _tool_logging("seed")
    return _report(seed_source(snowflake, source, seed, force=args.force).as_dict())


def _simulate(args: argparse.Namespace) -> int:
    snowflake, source = load_settings(SnowflakeSettings), load_settings(SourceSettings)
    defaults = load_settings(SimulateSettings)
    _tool_logging("simulate")
    report = simulate_changes(
        snowflake,
        source,
        inserts=defaults.inserts if args.inserts is None else args.inserts,
        updates=defaults.updates if args.updates is None else args.updates,
        invalid_rows=defaults.invalid_rows if args.invalid is None else args.invalid,
    )
    return _report(report.as_dict())


def _inject(_: argparse.Namespace) -> int:
    kafka, postgres = load_settings(KafkaSettings), load_settings(PostgresSettings)
    _tool_logging("inject")
    return _report(inject_bad_events(kafka, postgres))


def _replay(args: argparse.Namespace) -> int:
    kafka, postgres = load_settings(KafkaSettings), load_settings(PostgresSettings)
    consumer = load_settings(ConsumerSettings)
    _tool_logging("replay")
    report = replay_topic(kafka, consumer, postgres, rebuild=args.rebuild, force=args.force)
    _report(report.as_dict())
    return 0 if report.converged else 1


def _export_schema(args: argparse.Namespace) -> int:
    output: Path = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(envelope_json_schema(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}")
    return 0


def _check_config(_: argparse.Namespace) -> int:
    groups = (
        SnowflakeSettings,
        SourceSettings,
        AdapterSettings,
        KafkaSettings,
        ConsumerSettings,
        PostgresSettings,
        ApiSettings,
        LogSettings,
        SeedSettings,
        SimulateSettings,
    )
    problems: list[str] = []
    for group in groups:
        try:
            load_settings(group)
        except ConfigurationError as exc:
            problems.append(f"{group.__name__}: {exc}")
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 2
    rules = load_rules(load_settings(ConsumerSettings).quality_rules_path)
    return _report(
        {"settings": "ok", "quality_rules": {"fingerprint": rules.fingerprint, **rules.summary()}}
    )
