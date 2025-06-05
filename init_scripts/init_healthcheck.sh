#!/bin/bash

# Environment variables (set or default)
HOST=${POSTGRES_HOST:-localhost}
PORT=${POSTGRES_PORT:-5432}
USER=${POSTGRES_USER:-postgres}
DB=${POSTGRES_DB:-postgres}
export PGPASSWORD=${POSTGRES_PASSWORD:-yourpassword}

# Wait for DB to be ready (optional, recommended)
for i in {1..10}; do
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c '\q' > /dev/null 2>&1 && break
  echo "Waiting for PostgreSQL to start..."
  sleep 2
done

# Get list of tables in public schema
TABLES=$(psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -t -c \
"SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | xargs)

if [[ $? -eq 0 && -n "$TABLES" ]]; then
  echo "✅ PostgreSQL is healthy. Tables in 'public' schema:"
  echo "$TABLES" | tr ' ' ','
  exit 0
else
  echo "❌ PostgreSQL health check failed or no tables found in 'public' schema."
  exit 1
fi