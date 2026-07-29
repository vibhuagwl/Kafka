# Kafka Interview Q1–Q100 (Mapped to This Codebase)

Use this as an SDE-3 / Staff prep sheet. Every answer includes **internals** and, where we built it, a **code pointer**.

**Legend:** `✅ in repo` · `⚙️ docker/ops` · `📖 concept (not coded yet)`

---

## 1. Architecture & Storage

### Q1. Explain Kafka architecture from scratch

| Concept | Meaning | In this project |
|---------|---------|-----------------|
| **Broker** | A Kafka server process holding partitions | `docker-compose.yml` → `kafka-1/2/3` |
| **Topic** | Named stream of records | `KafkaTopics.java`, `KafkaTopicConfiguration` (`TopicBuilder`) |
| **Partition** | Ordered, append-only log slice of a topic | `ecommerce.kafka.topics.*-partitions: 6` |
| **Replica** | Copy of a partition on another broker | RF=3 in compose + TopicBuilder |
| **Leader** | Replica that serves produce/fetch | Elected by controller; see `--describe` |
| **Follower** | Replica that fetches from leader | ISR members |
| **Controller** | Manages leaders, partitions, membership | KRaft active controller |
| **KRaft Controller** | Controller role using Raft metadata log (no ZK) | `KAFKA_PROCESS_ROLES=broker,controller` |
| **ISR** | In-Sync Replicas caught up with leader | `min.insync.replicas=2` |
| **Producer** | Writes records | `KafkaEventPublisher` → `KafkaTemplate` |
| **Consumer** | Reads via poll | `@KafkaListener` inventory |
| **Consumer Group** | Set of consumers sharing partitions | `group-id: ecommerce-inventory-service` |
| **Offset** | Position in a partition log | Manual ack → commit; `__consumer_offsets` |
| **Log Segment** | File chunk of the partition log | Broker storage (see Q3) |

```
Producer → Leader Broker → append to partition log
                ↓ replicate
           Followers (ISR)
Consumer Group → Fetch from leaders → commit offsets
KRaft quorum → metadata (topics, leaders, configs)
```

### Q2. Why Kafka is faster than RabbitMQ?

1. **Sequential disk writes** — append-only logs, OS page cache friendly  
2. **Zero-copy** — `sendfile()` broker → socket, skip userspace copy  
3. **SendFile API** — kernel moves data disk/page-cache → NIC  
4. **Batch processing** — producer batches + fetch batches  
5. **Compression** — batch-level (lz4 here)  
6. **Pull model** — consumer controls rate (no broker push overload)  
7. **Partition parallelism** — scale consumers = scale partitions  

RabbitMQ: per-message routing, richer queue semantics, typically higher per-msg overhead.

**Code:** `application-kafka.yml` → `compression-type: lz4`, `linger-ms`, `batch-size`, `acks: all`.

### Q3. Kafka storage internals

- **Log segment** — file `00000000000000000000.log` holding a contiguous offset range  
- **Active segment** — currently appended; rolls on size/time  
- **Closed segment** — immutable; eligible for delete/compact  
- **Offset index (`.index`)** — sparse map offset → file position  
- **Time index (`.timeindex`)** — timestamp → offset (for seek by time)  
- **Log cleaner** — background thread for compaction/deletion  
- **Compaction** — keep latest value per key (`cleanup.policy=compact`)  

`OffsetManagementService.replayFromTimestamp` relies on time index on brokers.

### Q4. How Kafka stores 100TB?

Spread across **many partitions × many brokers**, each partition = segments on disk. Retention (`log.retention.hours` / bytes) or compaction bounds growth. Tiered storage (KIP) can offload old segments to object store. **Never one partition for 100TB** — rebalance/replication would be impossible.

**Ops:** RF=3 ⇒ ~300TB raw for 100TB logical.

### Q5. Broker disk full?

- Produce fails (`KafkaStorageException` / out of space)  
- Replication stalls → ISR shrinks  
- If `log.retention` can’t free space fast enough → broker unhealthy  
- Alerts on disk; expand volume / delete/retain / reassign partitions  

**Demo:** fill volume or watch under-replicated partitions after disk pressure.

### Q6. Recover after broker restart?

1. Load `meta.properties` / KRaft metadata  
2. Open log dirs, recover segments (truncate dirty unflushed tail if needed)  
3. Register with controller; become follower for owned replicas  
4. **Replica fetcher** catches up to leader → join ISR  
5. May become preferred leader again (`preferred-leader-election`)  

**Code/ops:** `./scripts/failover-demo.sh broker-recover kafka-1`

---

## 2. Producer Deep Dive

### Q7. Producer send flow

```
App (OrderController)
  → Serializer (JsonSerializer / Avro)
  → Partitioner (key hash | sticky | custom)
  → RecordAccumulator (per-partition Deque of batches)
  → Sender thread (I/O)
  → ProduceRequest → Partition Leader
```

**Code:** `KafkaEventPublisher.publishAsync` → `KafkaTemplate.send` → client `KafkaProducer`.

### Q8. RecordAccumulator

In-memory structure holding **incomplete batches** per partition. Sender drains ready batches (`batch.size` full or `linger.ms` elapsed). Memory capped by `buffer.memory`.

### Q9. `batch.size`

Max bytes per produce batch for a partition (default 16KB; we use `32768`). Larger ⇒ better throughput, higher latency/memory.

### Q10. `linger.ms`

Max wait to fill a batch before sending (`5` here). `0` = send ASAP (lower latency, worse batching).

### Q11. `buffer.memory`

Total bytes for RecordAccumulator (`67108864` = 64MB). Shared across partitions.

### Q12. Buffer full?

`send()` **blocks** up to `max.block.ms`, then throws `TimeoutException`. Back-pressure to app (don’t unbounded-async without limits).

### Q13. Producer retries

Retriable errors (`NetworkException`, `NotLeaderOrFollower`, etc.) retried up to `retries` / until `delivery.timeout.ms`. We set `retries: 2147483647` with delivery timeout bounding total time.

### Q14. Retry also fails?

Future completes exceptionally → our callback logs **PRODUCER FAIL**; sync path throws `ProducerFailureException` (`KafkaFailureClassifier.producer`). Message **not** in topic (or uncertain if timeout after accept — prefer idempotence).

### Q15. Idempotent producer

`enable.idempotence=true` ⇒ broker assigns **PID + epoch + sequence** per partition. Duplicates from retries are detected and ignored. Requires `acks=all`, `max.in.flight ≤ 5`.

**Code:** `application-kafka.yml` producer section.

### Q16. Avoid duplicate messages?

| Layer | Mechanism |
|-------|-----------|
| Broker | Idempotent producer (retry dupes) |
| App | `IdempotentEventProcessor` on `eventId` (Redis + local) |
| EOS | Transactions + `read_committed` |

### Q17. Retries vs idempotence

- **Retries** — resend on failure (can create duplicates without idempotence)  
- **Idempotence** — makes those retries safe (same seq rejected)

### Q18. Transactional producer

`transactional.id` → `beginTransaction` / produce multiple partitions / `sendOffsetsToTransaction` / `commitTransaction`. Atomic across topics+offsets. Prefix ready via `transactional-id-prefix` (enabled in TX module).

### Q19. Producer fencing

New producer with same `transactional.id` bumps **epoch**; old producer gets `ProducerFencedException` (zombie fencing). Critical for EOS after crash/restart.

### Q20. EOS (Exactly Once Semantics)

Idempotent produce + transactions + `isolation.level=read_committed` consumers. **Broker EOS ≠ business EOS** — still use outbox/idempotency for DB side effects (`IdempotentEventProcessor`).

---

## 3. Consumer Deep Dive

### Q21. Poll mechanism

Listener thread loops: `KafkaConsumer.poll(timeout)` → FetchRequest to leaders → deserialize → invoke `@KafkaListener` → commit per ack mode.

### Q22. Why continuously poll?

1. Fetch data  
2. Send heartbeats (modern clients: heartbeat thread, but poll still required for group membership / processing progress)  
3. Trigger rebalance callbacks  
4. Apply seek/pause  

### Q23. Stop polling?

Exceed `max.poll.interval.ms` → kicked from group → **rebalance** → another member gets partitions → possible duplicate processing until commit.

**Demo:** `POST /api/v1/ops/consumer/{id}/stop` (platform-admin).

### Q24. Heartbeat thread

Background thread sends Heartbeat to **group coordinator** every `heartbeat.interval.ms` (`15000`). If coordinator misses `session.timeout.ms` (`45000`) → member dead → rebalance.

### Q25. Rebalance

Group membership/partition ownership change. Triggers revoke → assign.

**Code:** `InterviewRebalanceListener` — commitSync on revoke-before-commit.

### Q26. Eager rebalance

Stop-the-world: **all** partitions revoked, then reassigned. Simple, causes lag spikes.

### Q27. Cooperative rebalance

Incremental: only partitions that must move are revoked. Default with **CooperativeStickyAssignor**.

**Code:** `AssignorStrategy.COOPERATIVE_STICKY`, `manualImmediateOffsetFactory`.

### Q28. Static membership

`group.instance.id` set → member survives brief restarts without rebalance (session within timeout). Reduces rebalance storms for k8s rolling restarts. 📖

### Q29. Consumer / Group Coordinator

One broker elected coordinator per group (hash of group id). Handles JoinGroup, SyncGroup, Heartbeat, OffsetCommit.

### Q30. Why offsets in Kafka?

Durable, scalable, consistent with cluster (topic `__consumer_offsets`). Survives consumer restarts; no external ZK offset store.

---

## 4. Offset Management

### Q31. Auto vs manual commit

| Auto | Manual |
|------|--------|
| Client commits on interval | App decides after processing |
| Easy; risk of loss/dupes | At-least-once control |

We use **`enable.auto.commit=false`** + `MANUAL_IMMEDIATE` in inventory.

### Q32. CommitSync vs CommitAsync

- **Sync** — blocks; retries; stronger guarantee on revoke (`InterviewRebalanceListener`)  
- **Async** — non-blocking; callback; may lose last commit on crash  

**Code:** `OffsetManagementService.commitSync` / `commitAsync`.

### Q33. When commitAsync loses data?

Crash after process, before async callback succeeds → restart from older offset → **duplicates** (hence idempotency). Or callback error ignored → gap risk if you advanced “logically” without commit.

### Q34. Offset reset latest vs earliest

When **no committed offset** / out of range (`auto.offset.reset`):

- **earliest** — from start (replay) — our default  
- **latest** — only new messages  
- **none** — throw  

### Q35. `__consumer_offsets`

Internal compacted topic. Key=(group,topic,partition), value=offset metadata. Compacted so latest commit wins.

### Q36. Offset topic corrupted?

Group may fail commits; reset offsets carefully; restore from backup; extreme cases reset group offsets (`kafka-consumer-groups --reset-offsets`). `OffsetManagementService.recoverCorruptedOffsets` seeks to beginning as a **demo** recovery path.

---

## 5. Partitioning

### Q37. Custom partitioner

Implement `Partitioner` / custom **assignor** for consumers. We built **`OrderAffinityAssignor`** (consumer-side assignment). Producer custom partitioner 📖 (key hash default).

### Q38. Why hash partitioning?

`hash(key) % numPartitions` ⇒ same key ⇒ same partition ⇒ **ordering per key** (orderId).

### Q39. Sticky partitioning

Producer sticky partitioner: keep using one partition for unkeyed/batch-friendly records until batch full — better batching than RR.

### Q40. Round-robin partitioning

Spray unkeyed records across partitions — balance, **no key ordering**.

### Q41. Hot partition

One key or skewed hash gets disproportionate traffic (celebrity customerId, null keys).

### Q42. Solve hot partition?

Salt keys, sub-partition by hash buckets, more partitions, change key design, separate topics for whales.

### Q43. Increasing partitions impact?

Breaks key→partition mapping for **new** hash; old data stays; ordering across old/new not global; consumers may rebalance; can’t assume same partition for historical key.

### Q44. Can partition count decrease?

**No** (not supported safely). Create new topic + migrate.

---

## 6. Replication

### Q45. Leader election

Controller picks new leader from **ISR** when leader fails (or preferred election).

### Q46. ISR

Replicas with lag under `replica.lag.time.max.ms`. Only ISR eligible for clean election.

### Q47. ISR shrinks?

Fewer replicas durable; if below `min.insync.replicas`, **acks=all produces fail** (durability over availability).

### Q48. `min.insync.replicas`

Minimum ISR size for `acks=all` success. We use **2** with RF=3.

### Q49. Leader crashes?

Controller elects ISR follower; producers/consumers refresh metadata (`NotLeaderOrFollower` → retry). Classified in `KafkaFailureClassifier`.

### Q50. Unclean leader election?

Elect non-ISR ⇒ **data loss** possible. We set `UNCLEAN_LEADER_ELECTION_ENABLE=false`.

### Q51. Preferred leader election?

Move leadership back to preferred replica (usually first in replica list / rack-aware).  

`./scripts/failover-demo.sh preferred-leader-election`

### Q52. Replica fetcher thread

Follower background threads Fetch from leader, append locally, update HW/ISR eligibility.

---

## 7. KRaft

### Q53. Why ZooKeeper removed?

Operational complexity, dual metadata systems, limited partition scale. KRaft = single metadata path.

### Q54. KRaft architecture

Nodes with `controller` role form Raft quorum; metadata log stores topics, configs, ACLs, member state. Brokers apply metadata snapshots/updates.

**Code:** `docker-compose.yml` combined `broker,controller` + `CONTROLLER_QUORUM_VOTERS`.

### Q55. Controller quorum

Odd number of voters (we use 3). Majority needed to commit metadata. Active controller = Raft leader.

### Q56. Metadata log

Raft-replicated log of cluster metadata events (topic create, leader change, …).

### Q57. Raft consensus

Leader election + replicated log + majority commit. Controllers agree on metadata before brokers observe it.

---

## 8. Performance

### Q58. Producer throughput

↑ `linger.ms`, `batch.size`, compression `lz4`/`zstd`, pipelining (`max.in.flight=5` with idempotence), more partitions, async send.

### Q59. Consumer lag

↑ concurrency (= partitions), `max.poll.records` tune, faster processing (`PartitionOrderedExecutor`), pause on back-pressure, fix hot keys, scale group members.

### Q60. Broker optimization

Page cache RAM, fast disks, enough file descriptors, tuned network threads / IO threads, avoid full GC, rack-aware placement.

### Q61. JVM tuning

G1/ZGC for broker, heap sized so **page cache** remains large (don’t give all RAM to heap).

### Q62–Q63. Compression

| Codec | Trait |
|-------|--------|
| gzip | High ratio, slow |
| snappy | Fast, moderate |
| **lz4** | Very fast (our default) |
| zstd | Great ratio + good speed |

**Fastest typically:** lz4 / snappy (cpu). Best ratio often zstd/gzip.

### Q64. Zero copy

Avoid copying payload broker userspace; `sendfile` to socket → less CPU, higher throughput.

### Q65. Page cache

OS caches log segments in RAM; hot reads/writes hit memory. Why sequential logs + leave RAM free matter.

---

## 9. Failure Handling

| Q | Scenario | Behavior | Project |
|---|----------|----------|---------|
| **66** | Broker crash | Leaders move; ISR shrinks | `failover-demo.sh broker-kill` |
| **67** | Controller crash | Quorum elects new active | `controller-status` |
| **68** | Producer crash | Unsent batches lost; idempotent/tx on retry | Publisher callbacks |
| **69** | Consumer crash | Rebalance; replay uncommitted | ops stop + idempotency |
| **70** | Network partition | Timeouts/retries; minority ISR | classifier BROKER |
| **71** | Split brain | Prevented by Raft majority + epoch fencing | KRaft |
| **72** | Rack awareness | Replicas across racks | `KAFKA_BROKER_RACK=rack-a/b/c` |

**Code:** `KafkaFailureClassifier`, `FailureSimulationController`.

---

## 10. Security (📖 mostly — Security module pending)

| Q | Topic | One-liner |
|---|-------|-----------|
| **73 SSL** | TLS broker↔client | Encrypt in transit |
| **74 SASL** | Auth framework | PLAIN/SCRAM/OAUTHBEARER |
| **75 SCRAM** | Salted challenge | Username/password without plain replay |
| **76 OAuth** | Token auth | IdP-issued bearer |
| **77 ACL** | Authorization | Topic/group ops per principal |
| **78 Encryption at rest** | Disk/volume | LUKS/cloud KMS; Kafka doesn’t encrypt log files by default |

---

## 11. Kafka Streams (📖)

| Q | Answer |
|---|--------|
| **79** | Streams = embedded library processing topics with topology; Consumer = manual poll/handlers |
| **80** | State stores for aggregations/joins |
| **81** | RocksDB local state; changelog topic for restore |
| **82** | Tumbling/hopping/session windows |
| **83** | KStream-KTable, stream-stream windowed joins |

---

## 12. CDC / Patterns (📖 + future modules)

| Q | Answer |
|---|--------|
| **84 Debezium** | Captures DB WAL → Kafka topics |
| **85 Outbox** | Write business row + outbox row same TX; CDC/poll publishes — avoids dual-write |
| **86 CDC vs polling** | CDC low-latency, ordered; polling simpler, laggy, misses deletes unless designed |
| **87 EOS + CDC** | Idempotent consumers + outbox ids; transactional publish where needed |

---

## 13. DLQ / Retry ✅

### Q88. Dead Letter Queue

Poison/exhausted messages → `dead-letter` via `DeadLetterPublishingRecoverer`.

### Q89. Retry topic

Staging topic for delayed retry (`retry` topic reserved; Spring `@RetryableTopic` later).

### Q90. Exponential backoff

`ExponentialBackOff(1000, 2.0)` in `DefaultErrorHandler` before DLQ.

### Q91. Poison message

Non-retryable (`IllegalArgumentException`, `PoisonMessageException`) → skip retries → DLQ. Reprocess via **replay-service**.

**Code:** `DeadLetterReprocessService`, `POST /api/v1/dlq/reprocess`.

---

## 14. Distributed Systems

| Q | Answer | Code |
|---|--------|------|
| **92 Pull** | Consumer controls rate; diverse speeds; back-pressure natural | poll loop |
| **93 CAP** | Kafka CP for metadata (Raft); availability vs durability tuned via ISR/min.ISR | min.ISR=2 |
| **94 Event sourcing** | State = fold of events; `event-store` topic reserved | topic |
| **95 CQRS** | Write model vs read projections from topics | pipeline services |
| **96 Saga** | Multi-step order: inventory→payment→ship via events; compensations on failure | services |
| **97 Outbox** | Dual-write safe publish | upcoming |
| **98 Inbox** | Deduped received events table | `IdempotentEventProcessor` ≈ inbox |
| **99 Ordering** | Per-partition only; key by `orderId` | aggregateId key |
| **100 Idempotency** | Process once effect despite at-least-once | `IdempotentEventProcessor` + force reprocess headers |

---

## Quick “Where in code?” Index

| Concern | File |
|---------|------|
| Topics / RF / minISR | `KafkaTopicConfiguration`, `application-kafka.yml`, `docker-compose.yml` |
| Producer | `KafkaEventPublisher`, `KafkaProducerConsumerConfiguration` |
| Consumer + ack | `OrderPlacedInventoryConsumer` |
| Assignors | `AssignorStrategy`, `OrderAffinityAssignor` |
| Rebalance | `InterviewRebalanceListener` |
| Offsets | `OffsetManagementService`, `OffsetCommitMode` |
| Dedup | `IdempotentEventProcessor` |
| Failures | `KafkaFailureClassifier`, `FailureSimulationController` |
| DLQ + reprocess | `DeadLetterPublishingRecoverer` config, `DeadLetterReprocessService`, replay-service |
| Avro key | `CustomAvroKeyDeserializer` |
| Threading | `PartitionOrderedExecutor` |
| KRaft / failover | `docker-compose.yml`, `scripts/failover-demo.sh` |

---

## How to practice aloud (Amazon/Netflix style)

1. Draw Q1 architecture on whiteboard in 90 seconds.  
2. Trace **one order** through Q7 + inventory consumer + offset commit.  
3. Narrate **broker kill** using ISR + min.ISR + producer retries.  
4. Explain **duplicate** after rebalance → idempotency (Q16/Q100).  
5. Explain **DLQ reprocess** with `force=true` (Q88–Q91).
