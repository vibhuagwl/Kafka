# Module 01 — Foundation: Maven Monorepo + Kafka 4.x KRaft + Spring Kafka Platform

> **Scope of this module:** project skeleton, 3-broker KRaft cluster, topic provisioning via
> Spring `KafkaAdmin` / `TopicBuilder`, shared `platform-kafka` auto-configuration,
> Order Service HTTP → Kafka produce path.
>
> **Stack (mandatory):** Java 21 · Spring Boot 3.4 · **Spring Kafka** · Maven · Kafka **4.x** KRaft · PostgreSQL · Redis · Docker Compose

---

## 1. Architecture Diagram (ASCII)

```
                    ┌──────────────────────────────────────────────────────────┐
                    │                 Docker Compose Network                    │
                    │                                                          │
  Customer HTTP     │   ┌─────────────┐     ProduceRequest (acks=all)           │
  POST /api/v1/     │   │ Order       │──────────────────────────────────┐     │
  orders ──────────►│   │ Service     │                                  │     │
                    │   │ :8081       │                                  ▼     │
                    │   └─────────────┘     ┌────────────────────────────────┐ │
                    │                       │   Kafka 4.x KRaft Quorum       │ │
                    │                       │                                │ │
                    │                       │  ┌────────┐ ┌────────┐ ┌─────┐ │ │
                    │                       │  │kafka-1 │ │kafka-2 │ │kaf-3│ │ │
                    │                       │  │broker+ │ │broker+ │ │b+c  │ │ │
                    │                       │  │contr.  │ │contr.  │ │     │ │ │
                    │                       │  │rack-a  │ │rack-b  │ │rack-c│ │ │
                    │                       │  └────────┘ └────────┘ └─────┘ │ │
                    │                       │  voters: 1@k1, 2@k2, 3@k3      │ │
                    │                       │  RF=3  min.ISR=2  unclean=off  │ │
                    │                       └────────────────────────────────┘ │
                    │   ┌──────────┐  ┌───────┐                                │
                    │   │Postgres  │  │ Redis │   (Inbox/Outbox/Idempotency     │
                    │   │ 16       │  │  7    │    land in later modules)       │
                    │   └──────────┘  └───────┘                                │
                    └──────────────────────────────────────────────────────────┘

Maven modules:
  common ──► platform-kafka ──► order-service (and later inventory/payment/...)
```

---

## 2. Sequence Diagram — Place Order (Module 1)

```
Customer          OrderController       KafkaEventPublisher      KafkaTemplate      Partition Leader
   │                     │                        │                    │                   │
   │ POST /orders        │                        │                    │                   │
   │────────────────────►│                        │                    │                   │
   │                     │ DomainEvent.of(...)    │                    │                   │
   │                     │ publishSync(orders)    │                    │                   │
   │                     │───────────────────────►│                    │                   │
   │                     │                        │ send(ProducerRecord)                   │
   │                     │                        │───────────────────►│                   │
   │                     │                        │                    │ Metadata (cached) │
   │                     │                        │                    │ hash(key)%parts   │
   │                     │                        │                    │ ProduceRequest    │
   │                     │                        │                    │──────────────────►│
   │                     │                        │                    │   wait ISR (acks=all)
   │                     │                        │                    │◄── ProduceResponse│
   │                     │                        │◄── SendResult ─────│                   │
   │  202 + partition/offset                      │                    │                   │
   │◄────────────────────│                        │                    │                   │
```

**Spring Kafka internals:** `KafkaTemplate.send` delegates to `KafkaProducer.send`.
The producer I/O sender thread batches records (`batch.size` / `linger.ms`), compresses (`lz4`),
and awaits `acks=all` (≥ `min.insync.replicas`). Idempotence attaches producer PID + seq.

---

## 3. Class Diagram (Module 1)

```
┌────────────────────────────┐
│ KafkaTopicConfiguration    │  @EnableKafka + NewTopic beans (TopicBuilder)
└─────────────┬──────────────┘
              │ uses
              ▼
┌────────────────────────────┐     ┌──────────────────────────┐
│ EcommerceKafkaProperties   │     │ KafkaTopics (constants)  │
└────────────────────────────┘     └──────────────────────────┘

┌────────────────────────────────┐
│ KafkaProducerConsumerConfiguration │
│  - ProducerFactory             │──► DefaultKafkaProducerFactory
│  - ConsumerFactory             │──► DefaultKafkaConsumerFactory
│  - KafkaTemplate               │──► JsonSerializer / ErrorHandlingDeserializer
└────────────────────────────────┘

┌────────────────────────────────┐
│ KafkaListenerContainerConfiguration │
│  - ConcurrentKafkaListenerContainerFactory (record/batch/manual)
│  - DefaultErrorHandler + DeadLetterPublishingRecoverer
│  - TracingRecordInterceptor (RecordInterceptor)
└────────────────────────────────┘

┌────────────────────┐       ┌──────────────────┐
│ KafkaEventPublisher│──────►│ KafkaTemplate    │
└────────────────────┘       └──────────────────┘
         ▲
         │
┌────────────────────┐
│ OrderController    │
└────────────────────┘
```

---

## 4. Folder Structure

```
ecommerce-order-system/
├── pom.xml                         # parent POM (Spring Boot 3.4.2)
├── docker-compose.yml
├── docker/postgres/init.sql
├── scripts/
│   ├── cluster-status.sh
│   └── failover-demo.sh
├── docs/modules/01-foundation-kraft-spring-kafka.md
├── common/                         # domain events, topic names, headers
├── platform-kafka/                 # Spring Kafka auto-config (MANDATORY abstraction layer)
│   └── .../config, publisher
├── order-service/                  # HTTP → Kafka (+ outbox)
├── inventory-service/              # consumer, inbox, retry/DLQ
├── replay-service/                 # DLQ reprocess API
├── platform-admin/                 # failure / pause-resume ops
└── streams-service/                # Kafka Streams metrics topology
```

---

## 5. Topics — Why Each Exists

| Topic | Why |
|-------|-----|
| `orders` | Source-of-truth stream for order placement; partition key = orderId |
| `inventory` | Inventory reservation / rejection events |
| `payment` | Payment auth / capture / failure |
| `shipping` | Shipment lifecycle |
| `notification` | Fan-out to email/SMS/push workers |
| `audit` | Immutable compliance / security trail |
| `retry` | Application / Spring RetryTopic staging (Module: Retry) |
| `dead-letter` | Poison messages after retries (`DeadLetterPublishingRecoverer`) |
| `reply-topic` | `ReplyingKafkaTemplate` request-reply correlation |
| `transaction-topic` | Multi-topic EOS / transaction demos |
| `event-store` | Append-only domain event log / CQRS read-model source |

Provisioned by **Spring Kafka** `KafkaAdmin` + `TopicBuilder` / `NewTopic` beans (not bash),
issuing `AdminClient#createTopics` → controller `CreateTopicsRequest`.

---

## 6. Kafka Cluster Concepts Mapped to Compose

| Concept | How we demonstrate it |
|---------|----------------------|
| KRaft (no ZK) | `KAFKA_PROCESS_ROLES=broker,controller` |
| Controller quorum | `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,...` |
| Leader election | Kill leader broker → ISR elects new leader |
| Follower replica | RF=3 ⇒ 1 leader + 2 followers per partition |
| ISR | `min.insync.replicas=2`; observe with `kafka-topics --describe` |
| Replication factor | `KAFKA_DEFAULT_REPLICATION_FACTOR=3` + TopicBuilder `.replicas(3)` |
| Min ISR | Cluster + per-topic `min.insync.replicas` |
| Preferred leader election | `./scripts/failover-demo.sh preferred-leader-election` |
| Rack awareness | `KAFKA_BROKER_RACK=rack-a\|b\|c` |
| Controller failover | Stop active controller; quorum elects new one (`kafka-metadata-quorum.sh`) |
| Broker failover / recovery | `./scripts/failover-demo.sh broker-kill\|broker-recover` |
| Graceful shutdown / restart | `broker-restart` (SIGTERM → controlled shut down) |

---

## 7. Configuration

- Shared defaults: `platform-kafka/.../application-kafka.yml`
- Service import: `spring.config.import: optional:classpath:application-kafka.yml`
- Producer: `acks=all`, idempotence on, lz4, linger 5ms, max.in.flight=5 (safe with idempotence)
- Consumer: `enable.auto.commit=false`, `isolation.level=read_committed`
- Topics: `auto.create.topics.enable=false` — only Spring `KafkaAdmin` creates them

---

## 8. Docker Setup

```bash
docker compose up -d
./scripts/cluster-status.sh
./scripts/failover-demo.sh controller-status
```

Bootstrap for clients (host): `localhost:9092,localhost:9094,localhost:9096`

---

## 9. Failure Scenarios (Module 1)

1. **Broker crash** — leaders on that broker move; produces with `acks=all` pause briefly then resume on new leaders.
2. **Controller crash** — remaining voters elect new active controller; metadata writes stall briefly.
3. **min.ISR not met** — produces with `acks=all` fail (NotEnoughReplicas); durability > availability.
4. **Network partition** — under-replicated partitions; unclean election OFF ⇒ no divergent leaders.
5. **Order service crash after accept, before produce** — no Kafka event (at-most-once at edge); Outbox module fixes this.
6. **Serialize failure** — `JsonSerializer` throws before network; no partial broker write.
7. **KafkaAdmin fail-fast** — service won't boot if brokers unreachable (`spring.kafka.admin.fail-fast=true`).

---

## 10. Interview Questions (Module 1)

### Why Spring Kafka / KafkaAdmin instead of bash topics?
Declarative, versioned with the app, testable with EmbeddedKafka/Testcontainers, consistent with CI. Ops orgs may still own topics via Terraform — discuss ownership boundaries.

### What does `TopicBuilder` map to on the wire?
`AdminClient.createTopics` → Metadata discover controller → `CreateTopicsRequest` to KRaft controller → metadata log append → brokers apply.

### Why RF=3 and min.ISR=2?
Survive one broker loss without accepting acks=all produces that aren't durable on ≥2 brokers. Trade-off: need ≥2 brokers up to produce.

### Amazon-style cross question
"If rack-a dies and all leaders were preferred on rack-a, what happens?" → Preferred replicas may be offline; leaders move to other racks; preferred election later restores.

### Netflix-style
"How do you prevent cascading produce failures during a broker bounce?" → idempotent producer + retries + delivery.timeout.ms covering bounce window; circuit break at app if needed.

### Uber-style
"Why disable auto topic create?" → avoid accidental RF=1 topics, schema sprawl, partition surprises in prod.

### Walmart-style
"How do you prove ISR after deploy?" → `kafka-topics --describe`, under-replicated-partitions metric, Grafana (Monitoring module).

### JPMorgan-style
"Where is the durability guarantee enforced?" → broker `min.insync.replicas` + producer `acks=all` + unclean election disabled; app cannot override broker min.ISR downward for durability.

### Follow-ups
- Sticky vs cooperative rebalance? (Consumer module)
- Exactly-once with DB? (Transactions + Outbox modules)
- Why pull model? (Consumer / Pull module)

---

## 11. Optimizations

- `linger.ms` + `batch.size` for throughput
- `lz4` compression (CPU vs network)
- Partition count ≥ max expected consumer concurrency per group
- Rack-aware replica placement reduces correlated failure risk

## 12. Tradeoffs

| Choice | Pro | Con |
|--------|-----|-----|
| Combined broker+controller | Simpler local cluster | Blast radius couples control + data plane |
| Spring KafkaAdmin topics | App-owned | Can fight central Kafka governance |
| acks=all + min.ISR=2 | Durability | Higher produce latency / availability coupling |
| JsonSerializer | Debuggable | Larger payloads than Avro/Protobuf (Serialization module) |

## 13. Production Considerations

- Separate controller quorum from brokers at large scale
- TLS/SASL (Security module)
- Quotas, rack awareness in multi-AZ
- Topic ACLs
- Don't use `fail-fast` alone — readiness probes + rolling deploy

## 14. Common Mistakes

- RF=1 in "prod-like" compose
- acks=1 with "we need durability" story
- Enabling unclean leader election
- Relying on auto-create topics
- Same transactional.id across horizontally scaled pods without suffixing
- Blocking HTTP thread on sync produce without timeouts (use async + 202 in later hardening)

## 15. Real-World Company Examples

- **LinkedIn** — Kafka origin; KRaft migration off ZK; heavy use of RF and ISR ops
- **Uber** — multi-AZ Kafka; rack awareness; strict topic governance
- **Netflix** — keyed partitioning for stream processors; resilience to broker bounce
- **Amazon** — order/event pipelines; durability vs latency product tradeoffs
- **Stripe** — exactly-once *effects* via idempotency keys (app-level) even when broker EOS not used everywhere

---

## How to Run (Module 1)

```bash
docker compose up -d
mvn -pl order-service -am spring-boot:run
curl -s localhost:8081/api/v1/orders -H 'Content-Type: application/json' -d '{
  "customerId":"cust-1",
  "currency":"USD",
  "totalAmount":99.99,
  "lines":[{"sku":"SKU-1","quantity":1,"unitPrice":99.99}]
}'
./scripts/cluster-status.sh
```

---

## Next Module

**Module 02 — Partitions & Custom Partitioner** (ordering, sticky/round-robin, custom assigner prelude)
 then Producer framework deep dive (interceptors, routing template, request-reply).
