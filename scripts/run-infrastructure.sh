#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== 1. Starting Redpanda & PostgreSQL Containers ==="
docker compose -f "$DIR/docker/docker-compose.yml" up -d

echo "Waiting for Redpanda to be healthy..."
until docker compose -f "$DIR/docker/docker-compose.yml" exec -T redpanda rpk cluster health | grep -i healthy; do
    echo "Waiting for Redpanda..."
    sleep 2
done

echo "Infrastructure is UP!"
echo "- Redpanda Broker: localhost:9092"
echo "- Redpanda Console UI: http://localhost:8080"
echo "- PostgreSQL Serving DB: localhost:5432"

