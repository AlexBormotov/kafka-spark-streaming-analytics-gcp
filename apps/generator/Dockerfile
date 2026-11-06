# Build context: repository root (the uv workspace lock and schemas live there).
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.9.24 /uv /bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH=/app/.venv/bin:$PATH

COPY pyproject.toml uv.lock ./
COPY apps/streaming/pyproject.toml apps/streaming/pyproject.toml
COPY apps/generator apps/generator
RUN uv sync --frozen --no-dev --package qoe-generator --no-editable

COPY schemas schemas
ENV GEN_SCHEMA_DIR=/app/schemas

USER 65534
ENTRYPOINT ["python", "-m", "qoe_generator"]
