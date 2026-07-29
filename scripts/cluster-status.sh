#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== Metadata quorum ==="
docker compose exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:19092 describe --status || true

echo
echo "=== Topics ==="
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --list || true

echo
echo "=== Describe orders (RF, ISR, leaders, racks via replicas) ==="
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --topic orders || true
