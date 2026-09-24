"""FastAPI application factory for the event-inspection API."""

import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import psycopg
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from switch_pipeline import __version__
from switch_pipeline.api.routes import build_router
from switch_pipeline.observability import bound_contextvars, get_logger
from switch_pipeline.settings import ApiSettings, KafkaSettings, PostgresSettings
from switch_pipeline.transport.lag import ConsumerLagInspector

log = get_logger(__name__)

_DESCRIPTION = """
Inspect the change events streamed from Snowflake through Kafka into PostgreSQL:
the append-only event log, the current state per entity, quarantined records
and pipeline statistics (lag, watermark, consumer lag, convergence checksums).
"""


def create_app(*, postgres: PostgresSettings, api: ApiSettings, kafka: KafkaSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Read-only sessions: the API cannot modify the sink even by mistake.
        pool = AsyncConnectionPool(
            postgres.conninfo(application_name="switch-api", read_only=True),
            min_size=postgres.pool_min_size,
            max_size=postgres.pool_max_size,
            open=False,
            check=AsyncConnectionPool.check_connection,
            name="api",
        )
        await pool.open(wait=False)  # start even if the database is still coming up
        inspector = ConsumerLagInspector(kafka)
        app.state.pool = pool
        app.state.lag_inspector = inspector
        log.info("api_started", port=api.port, auth="bearer" if api.auth_token else "none")
        if api.auth_token is None:
            log.warning("api_auth_disabled", hint="set API_AUTH_TOKEN outside local development")
        try:
            yield
        finally:
            await pool.close()
            inspector.close()

    app = FastAPI(
        title="Switch event inspection API",
        version=__version__,
        description=_DESCRIPTION,
        lifespan=lifespan,
    )
    app.middleware("http")(_request_context)
    app.add_exception_handler(PoolTimeout, _database_unavailable)
    app.add_exception_handler(psycopg.OperationalError, _database_unavailable)
    app.include_router(build_router(api))
    return app


async def _request_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_id = request.headers.get("x-request-id") or str(uuid4())
    started = time.perf_counter()
    with bound_contextvars(request_id=request_id):
        try:
            response = await call_next(request)
        except Exception:
            log.exception("request_failed", method=request.method, path=request.url.path)
            response = JSONResponse(
                {"detail": "internal server error", "request_id": request_id},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        response.headers["x-request-id"] = request_id
        log.info(
            "http_request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
        )
        return response


async def _database_unavailable(request: Request, exc: Exception) -> Response:
    log.warning("database_unavailable", path=request.url.path, error=str(exc).strip())
    return JSONResponse(
        {"detail": "database unavailable"}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE
    )
