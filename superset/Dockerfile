FROM apache/superset:latest

USER root

RUN UV_CACHE_DIR=/tmp/uv-cache \
    uv pip install \
    --python /app/.venv/bin/python \
    clickhouse-connect \
    psycopg2-binary

USER superset