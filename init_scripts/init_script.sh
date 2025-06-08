#!/bin/bash

# Create Postgres DB and Airflow containers
docker compose -p fantasy_basketball --env-file ../.env up airflow-init -d
docker-compose -p fantasy_basketball --env-file ../.env up -d

echo "Waiting for container to become healthy..."

# Wait up to 3 minute (36 attempts, 5 seconds apart)
for i in {1..36}; do
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

#Build fantasy etl pipeline image
docker build -f ../pipeline/fantasy_etl.Dockerfile -t fantasy-etl-image ../

