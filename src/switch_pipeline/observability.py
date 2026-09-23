"""Structured JSON logging with correlation ids carried in context variables.

``bound_contextvars(batch_id=..., event_id=...)`` attaches ids to every log line
emitted inside the block, including lines from third-party libraries routed
through the standard ``logging`` module.
"""

import logging
import sys
from typing import TextIO

import structlog
from structlog.contextvars import bound_contextvars
from structlog.typing import Processor

from switch_pipeline.settings import LogSettings

__all__ = ["bound_contextvars", "configure_logging", "get_logger"]

get_logger = structlog.stdlib.get_logger

# Libraries that are chatty at INFO; kept at WARNING whatever LOG_LEVEL says.
_QUIET_LOGGERS = ("snowflake.connector", "urllib3", "botocore", "uvicorn.access", "asyncio")


def configure_logging(settings: LogSettings, *, service: str, stream: TextIO | None = None) -> None:
    """Route structlog and stdlib logging to ``stream`` (stdout by default).

    CLI tools log to stderr so that their stdout carries only the JSON report.
    """
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    render: list[Processor]
    if settings.format == "json":
        # Frame locals would copy payloads and connection details into the logs.
        tracebacks = structlog.processors.ExceptionRenderer(
            structlog.tracebacks.ExceptionDictTransformer(show_locals=False)
        )
        render = [tracebacks, structlog.processors.JSONRenderer()]
    else:
        render = [structlog.dev.ConsoleRenderer()]

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *render],
    )
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.level)
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service)
