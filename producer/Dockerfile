# syntax=docker/dockerfile:1
# Build from the repo root:  docker build -f producer/Dockerfile -t crypto-recon-producer .

# ---- build: resolve the producer and its workspace deps into /opt/venv ----
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package crypto-recon-producer

# ---- runtime: Python + the venv only; no uv, no source, no dev tools ----
FROM python:3.12-slim
RUN useradd --create-home --uid 10001 producer \
    && mkdir -p /app/data \
    && chown producer /app/data
COPY --from=builder /opt/venv /opt/venv
COPY config/products.yml /app/config/products.yml
WORKDIR /app
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1
USER producer
# Exec form: Python is PID 1 and receives SIGTERM from `docker stop` (graceful flush, FR-P8).
ENTRYPOINT ["python", "-m", "crypto_recon_producer"]
