# Module 02 — Avro Key Deserializer, Schema Registry, Assignors, Offsets, Failures

## What was added

### Avro + Schema Registry
- `CustomAvroKeySerializer` / `CustomAvroKeyDeserializer` (Confluent wire: magic + schemaId + avro)
- `OrderKey.avsc` / `OrderPlacedEvent.avsc`
- Schema Registry in Docker (`localhost:8082`)
- `POST /api/v1/orders/avro` on Order Service

### Partition assignors (consumer factories)
| Bean | Strategy |
|------|----------|
| `rangeAssignorListenerFactory` | RangeAssignor |
| `roundRobinAssignorListenerFactory` | RoundRobinAssignor |
| `stickyAssignorListenerFactory` | StickyAssignor |
| `cooperativeStickyListenerFactory` | CooperativeStickyAssignor |
| `orderAffinityAssignorListenerFactory` | **Custom** `OrderAffinityAssignor` |
| `manualImmediateOffsetFactory` | CooperativeSticky + MANUAL_IMMEDIATE ack |

### Offset management
`OffsetManagementService` — seek, replay by offset/timestamp, commitSync/commitAsync, external offset map, corruption recovery.

### Duplicates
`IdempotentEventProcessor` — local + Redis `SET NX` on `eventId`.

### Multi-threading
`PartitionOrderedExecutor` — per-partition queue + virtual threads + back-pressure semaphore.

### Failures
`KafkaFailureClassifier` + sealed exceptions; `FailureSimulationController` (`:8090`); Docker failover scripts for broker/controller/leader.

### Inventory consumer
`OrderPlacedInventoryConsumer` — manual ack, dedup, partition workers, poison → DLQ.

## Interview cheat sheet

| Failure | What happens | Code / demo |
|---------|--------------|-------------|
| Producer fail | Retries + delivery.timeout; idempotent PID/seq | `KafkaEventPublisher` callbacks |
| Broker fail | Leaders move; ISR shrinks | `./scripts/failover-demo.sh broker-kill` |
| Leader fail | `NotLeaderOrFollower` → metadata refresh | kill leader broker |
| Controller fail | Quorum elects new active controller | `controller-status` after kill |
| Consumer fail | Stop listener → rebalance | `POST /api/v1/ops/consumer/{id}/stop` |
| Duplicate consume | Rebalance before commit | IdempotentEventProcessor |
| Poison message | Non-retryable → DLQ | key contains `POISON` |

## Run

```bash
docker compose up -d
mvn -pl order-service,inventory-service,platform-admin -am spring-boot:run
# or run each module in separate terminals
```
