#!/bin/bash

# Create Postgres DB
docker-compose -p fantasy_basketball -f postgres-docker-compose.yaml --env-file ../.env up -d

echo "Waiting for container to become healthy..."

# Wait up to 1 minute (12 attempts, 5 seconds apart)
for i in {1..12}; do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' fantasy_basketball_postgres)
  echo "Attempt $i: Health status is '$STATUS'"

  if [ "$STATUS" == "healthy" ]; then
    echo "✅ Container is healthy!"
    break
  fi

  sleep 5
done

echo "=== Healthcheck Log ==="
docker inspect --format='{{json .State.Health.Log}}' fantasy_basketball_postgres