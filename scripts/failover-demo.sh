#!/usr/bin/env bash
# Failover / recovery demos for Module 1 interview prep.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

cmd="${1:-help}"

case "$cmd" in
  broker-kill)
    broker="${2:-kafka-1}"
    echo "Stopping $broker (simulate broker crash)..."
    docker compose stop "$broker"
    echo "Watch ISR shrink / leaders move: ./scripts/cluster-status.sh"
    ;;
  broker-recover)
    broker="${2:-kafka-1}"
    echo "Starting $broker (broker recovery)..."
    docker compose start "$broker"
    ;;
  broker-restart)
    broker="${2:-kafka-1}"
    echo "Graceful restart $broker..."
    docker compose restart "$broker"
    ;;
  controller-status)
    docker compose exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server kafka-1:19092 describe --status
    ;;
  preferred-leader-election)
    echo "Triggering preferred replica election..."
    docker compose exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh \
      --bootstrap-server kafka-1:19092 \
      --election-type preferred \
      --all-topic-partitions || true
    ;;
  help|*)
    cat <<EOF
Usage: $0 <command>

  broker-kill [kafka-1|kafka-2|kafka-3]
  broker-recover [kafka-1|kafka-2|kafka-3]
  broker-restart [kafka-1|kafka-2|kafka-3]
  controller-status
  preferred-leader-election
EOF
    ;;
esac
