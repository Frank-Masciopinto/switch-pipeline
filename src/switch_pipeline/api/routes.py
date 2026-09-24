"""HTTP endpoints. Limits and defaults come from ApiSettings (i.e. from .env)."""

import secrets
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg_pool import AsyncConnectionPool
from pydantic import AwareDatetime, SecretStr
from starlette.concurrency import run_in_threadpool

from switch_pipeline.api.schemas import (
    BatchSummary,
    ConsumerLag,
    CurrentState,
    EntityView,
    EventCounts,
    EventPage,
    EventRecord,
    LagStats,
    LagSummary,
    LatestTimestamps,
    QuarantineCounts,
    QuarantinePage,
    QuarantineRecord,
    SinkChecksums,
    Stats,
    Watermark,
)
from switch_pipeline.domain.envelope import EventType
from switch_pipeline.domain.quarantine import QuarantineReason
from switch_pipeline.settings import ApiSettings
from switch_pipeline.sink import reads
from switch_pipeline.transport.lag import ConsumerLagInspector


def get_pool(request: Request) -> AsyncConnectionPool:
    pool: AsyncConnectionPool = request.app.state.pool
    return pool


def get_lag_inspector(request: Request) -> ConsumerLagInspector:
    inspector: ConsumerLagInspector = request.app.state.lag_inspector
    return inspector


Pool = Annotated[AsyncConnectionPool, Depends(get_pool)]
LagInspector = Annotated[ConsumerLagInspector, Depends(get_lag_inspector)]


_bearer = HTTPBearer(auto_error=False)


def require_token(expected: SecretStr) -> Callable[..., Awaitable[None]]:
    async def check(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    ) -> None:
        token = credentials.credentials if credentials else ""
        if not secrets.compare_digest(token.encode(), expected.get_secret_value().encode()):
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "missing or invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return check


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter()  # health probes: always open
    protected = APIRouter(  # payloads: behind the token when API_AUTH_TOKEN is set
        dependencies=[Depends(require_token(settings.auth_token))] if settings.auth_token else []
    )
    PageSize = Annotated[int, Query(ge=1, le=settings.page_size_max)]  # noqa: N806

    @router.get("/healthz", tags=["ops"], summary="Liveness")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/readyz", tags=["ops"], summary="Readiness (database reachable)")
    async def readyz(pool: Pool, response: Response) -> dict[str, str]:
        try:
            async with pool.connection(timeout=2) as conn:
                await conn.execute("SELECT 1")
        except Exception:  # any failure means "not ready"; details are in the logs
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "unavailable"}
        return {"status": "ready"}

    @protected.get("/events", response_model=EventPage, tags=["events"], summary="Streamed events")
    async def list_events(
        pool: Pool,
        entity_key: Annotated[str | None, Query(max_length=512)] = None,
        entity_type: Annotated[str | None, Query(max_length=64)] = None,
        event_type: EventType | None = None,
        batch_id: Annotated[
            UUID | None, Query(description="Adapter batch id, for end-to-end tracing.")
        ] = None,
        occurred_after: Annotated[
            AwareDatetime | None, Query(description="Inclusive, ISO-8601 with offset.")
        ] = None,
        occurred_before: Annotated[
            AwareDatetime | None, Query(description="Exclusive, ISO-8601 with offset.")
        ] = None,
        limit: PageSize = settings.page_size_default,
        cursor: Annotated[
            str | None, Query(description="`next_cursor` of the previous page.")
        ] = None,
    ) -> EventPage:
        if occurred_after and occurred_before and occurred_after >= occurred_before:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "occurred_after must be earlier than occurred_before",
            )
        filters = reads.EventFilters(
            entity_key=entity_key,
            entity_type=entity_type,
            event_type=event_type,
            batch_id=batch_id,
            occurred_after=occurred_after,
            occurred_before=occurred_before,
        )
        async with pool.connection() as conn:
            rows, next_before = await reads.list_events(
                conn, filters, limit=limit, before=_cursor_param(cursor)
            )
        return EventPage(
            items=[EventRecord.from_row(row) for row in rows],
            next_cursor=None if next_before is None else reads.encode_cursor(next_before),
        )

    @protected.get("/events/{event_id}", response_model=EventRecord, tags=["events"])
    async def get_event(pool: Pool, event_id: UUID) -> EventRecord:
        async with pool.connection() as conn:
            row = await reads.get_event(conn, event_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "event not found")
        return EventRecord.from_row(row)

    @protected.get(
        "/entities/{key}",
        response_model=EntityView,
        tags=["entities"],
        summary="Current state and history of one entity",
        responses={404: {"description": "Unknown key"}, 409: {"description": "Ambiguous key"}},
    )
    async def get_entity(
        pool: Pool,
        key: str,
        entity_type: Annotated[str | None, Query(max_length=64)] = None,
        history_limit: Annotated[
            int, Query(ge=1, le=settings.page_size_max)
        ] = settings.entity_history_limit,
    ) -> EntityView:
        async with pool.connection() as conn:
            if entity_type is None:
                types = await reads.entity_types_for_key(conn, key)
                if len(types) > 1:
                    raise HTTPException(
                        status.HTTP_409_CONFLICT,
                        f"key exists for several entity types {types}; pass ?entity_type=",
                    )
                entity_type = types[0] if types else None
            current = history = None
            truncated = False
            if entity_type is not None:
                current = await reads.get_current_state(conn, entity_type, key)
                history, truncated = await reads.entity_history(
                    conn, entity_type, key, limit=history_limit
                )
            quarantined, _ = await reads.list_quarantine(
                conn, reason=None, entity_key=key, limit=history_limit, before=None
            )
        if current is None and not history and not quarantined:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "no events for this key")
        return EntityView(
            entity_type=entity_type,
            entity_key=key,
            current=CurrentState(**current) if current else None,
            history=[EventRecord.from_row(row) for row in history or []],
            history_truncated=truncated,
            quarantined=[QuarantineRecord.from_row(row) for row in quarantined],
        )

    @protected.get(
        "/quarantine",
        response_model=QuarantinePage,
        tags=["quality"],
        summary="Records rejected by schema, duplicate or quality checks",
    )
    async def list_quarantine(
        pool: Pool,
        reason: QuarantineReason | None = None,
        entity_key: Annotated[str | None, Query(max_length=512)] = None,
        limit: PageSize = settings.page_size_default,
        cursor: str | None = None,
    ) -> QuarantinePage:
        async with pool.connection() as conn:
            rows, next_before = await reads.list_quarantine(
                conn,
                reason=reason.value if reason else None,
                entity_key=entity_key,
                limit=limit,
                before=_cursor_param(cursor),
            )
        return QuarantinePage(
            items=[QuarantineRecord.from_row(row) for row in rows],
            next_cursor=None if next_before is None else reads.encode_cursor(next_before),
        )

    @protected.get("/stats", response_model=Stats, tags=["stats"], summary="Pipeline observability")
    async def stats(
        pool: Pool,
        lag_inspector: LagInspector,
        checksums: Annotated[
            bool,
            Query(description="Include convergence checksums (scans the sink; for verification)."),
        ] = False,
    ) -> Stats:
        async with pool.connection() as conn:
            data = await reads.sink_stats(
                conn, lag_sample_size=settings.lag_sample_size, include_checksums=checksums
            )
        consumer_lag = await run_in_threadpool(lag_inspector.snapshot)
        totals, lag = data["totals"], data["lag"]
        return Stats(
            generated_at=datetime.now(UTC),
            events=EventCounts(
                total=sum(data["by_type"].values()),
                by_type=data["by_type"],
                with_quality_warnings=totals["warned"],
            ),
            entities=totals["entities"],
            duplicates_skipped=totals["duplicates"],
            quarantine=QuarantineCounts(
                total=sum(data["by_reason"].values()), by_reason=data["by_reason"]
            ),
            lag_seconds=LagSummary(
                sample_size=lag["sample_size"],
                occurred_to_processed=_lag_stats(lag, "end_to_end"),
                occurred_to_captured=_lag_stats(lag, "capture"),
                captured_to_processed=_lag_stats(lag, "delivery"),
            ),
            latest=LatestTimestamps(
                occurred_at=totals["latest_occurred"], processed_at=totals["latest_processed"]
            ),
            watermarks=[Watermark(**row) for row in data["watermarks"]],
            recent_batches=[BatchSummary(**row) for row in data["batches"]],
            consumer_lag=ConsumerLag.model_validate(asdict(consumer_lag)),
            checksums=SinkChecksums(**data["checksums"]) if data["checksums"] else None,
        )

    router.include_router(protected)
    return router


def _lag_stats(row: dict[str, Any], prefix: str) -> LagStats:
    def rounded(name: str) -> float | None:
        value = row[f"{prefix}_{name}"]
        return None if value is None else round(float(value), 3)

    return LagStats(avg=rounded("avg"), p50=rounded("p50"), p95=rounded("p95"), max=rounded("max"))


def _cursor_param(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    try:
        return reads.decode_cursor(cursor)
    except reads.InvalidCursorError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
