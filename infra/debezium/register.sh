#!/usr/bin/env bash
# Register (or update) the Debezium Postgres connector against Kafka Connect.
# Idempotent: PUT /connectors/<name>/config creates on first run, updates after.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="crypto-connector"
CONFIG="$(dirname "$0")/crypto-connector.json"

echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 2
done

echo "registering connector '${NAME}' ..."
curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data @"${CONFIG}" \
  "${CONNECT_URL}/connectors/${NAME}/config" >/dev/null

echo "done. connector status:"
curl -s "${CONNECT_URL}/connectors/${NAME}/status" | python3 -m json.tool 2>/dev/null \
  || curl -s "${CONNECT_URL}/connectors/${NAME}/status"
