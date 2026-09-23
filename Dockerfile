# syntax=docker/dockerfile:1
# One image for every service (adapter, consumer, api, tools): the command differs.
# Base images are pinned in .env and passed in by docker compose.
ARG PYTHON_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /build
# Dependencies first, so this layer is reused until uv.lock changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
RUN groupadd --system --gid 10001 switch \
    && useradd --system --uid 10001 --gid switch --home-dir /app --no-create-home switch
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
COPY config ./config
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER switch
ENTRYPOINT ["switch-pipeline"]
CMD ["--help"]
