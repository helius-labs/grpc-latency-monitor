#!/bin/bash
# Applies the schema on the container's first start. Only the Up section of each
# migration runs; Goose's history table is not written, so do not point Goose at
# this database later. Production installs apply migrations with Goose (sql/README.md).
set -euo pipefail
for migration in /migrations/*.sql; do
  sed '/^-- +goose Down/,$d' "$migration" |
    clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
      --database "$CLICKHOUSE_DB" --multiquery
done
