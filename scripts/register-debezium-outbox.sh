#!/usr/bin/env bash
# Register Debezium outbox CDC connector (Q84–Q87)
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8084}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Waiting for Connect at $CONNECT_URL ..."
for i in {1..60}; do
  if curl -sf "$CONNECT_URL/" >/dev/null; then
    break
  fi
  sleep 2
done

curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$ROOT/docker/debezium/orders-outbox-connector.json" \
  "$CONNECT_URL/connectors" || \
curl -sf -X PUT -H "Content-Type: application/json" \
  --data @"$ROOT/docker/debezium/orders-outbox-connector.json" \
  "$CONNECT_URL/connectors/orders-outbox-connector/config"

echo
echo "Connectors:"
curl -sf "$CONNECT_URL/connectors"
echo
