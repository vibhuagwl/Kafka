# Implementation coverage vs Q1–Q100

Updated after full-feature push. Legend: ✅ code · ⚙️ ops/compose · 🔶 partial

| Area | Status | Where |
|------|--------|-------|
| Architecture / KRaft / ISR / RF | ✅⚙️ | docker-compose, TopicBuilder |
| Producer send / batch / linger / idempotence | ✅ | KafkaEventPublisher, application-kafka.yml |
| Custom Partitioner (hash/sticky/RR) | ✅ | `OrderAwarePartitioner` |
| Transactions / EOS / fencing | ✅ | `transactions-enabled`, KafkaTransactionManager, ExactlyOnceKafkaService |
| Consumer poll / ack / rebalance | ✅ | Inventory listener, InterviewRebalanceListener |
| Assignors (Range/Sticky/Coop/Custom) | ✅ | AssignorStrategy factories |
| Static membership | ✅ | `group-instance-id` |
| Offsets manual/sync/async/seek | ✅ | OffsetManagementService |
| Dedup / Inbox / Idempotency | ✅ | IdempotentEventProcessor, InboxService |
| Outbox | ✅ | OutboxEventEntity, OutboxPoller, POST /orders/outbox |
| RetryableTopic + exponential backoff | ✅ | @RetryableTopic + SpringRetryTopicConfiguration |
| DLQ + reprocess | ✅ | DeadLetter* + replay-service |
| Avro key + Schema Registry | ✅ | CustomAvroKey* |
| Multi-threading / back-pressure | ✅ | PartitionOrderedExecutor |
| Failures (broker/controller/consumer) | ✅⚙️ | classifier, admin API, failover scripts |
| Kafka Streams (state/window/join) | ✅ | streams-service |
| Debezium CDC | ⚙️ | connect service + register script |
| Security SSL/SASL/SCRAM/ACL | 🔶 | client wiring ✅ + security overlay/docs (certs manual) |
| Encryption at rest | 📖 | docs only (volume/KMS) |
| JVM broker tuning / 100TB | 📖 | interview explanation |

## New endpoints / commands

```bash
# Outbox
curl -X POST localhost:8081/api/v1/orders/outbox -H 'Content-Type: application/json' -d '{...}'

# Debezium
./scripts/register-debezium-outbox.sh

# Streams
mvn -pl streams-service -am spring-boot:run

# TX on order-service already: ecommerce.kafka.transactions-enabled=true
```
