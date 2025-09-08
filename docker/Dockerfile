FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Set environment variables
ENV DAGSTER_VERSION=1.11.9
ENV UV_SYSTEM_PYTHON=1

# Set working directory
WORKDIR /app

RUN apt update && apt install -y git

# Copy the source code
COPY ../ /app/dagster-clickhouse/

# Install dependencies using uv
RUN uv pip install \
    dagster \
    dagster-azure \
    dagster-postgres \
    dagster-k8s \
    dagster-aws \
    dagster-gcp \
    dagster-graphql \
    dagster-webserver \
    grpcio==1.57.0

# Install dagster-clickhouse from local source
RUN cd /app/dagster-clickhouse && uv pip install -e .
