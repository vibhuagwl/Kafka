# E-Commerce Order Processing System (Interview-Grade Kafka)

Java 21 · Spring Boot 3.4 · **Spring Kafka (mandatory)** · Kafka **4.x KRaft** · **Maven** · Avro · Schema Registry · PostgreSQL · Redis · Docker Compose

> Built **incrementally by module**. Do not expect every Spring Kafka feature in Module 1.

## Module Roadmap

| # | Module | Status |
|---|--------|--------|
| 01 | Foundation: Maven monorepo, KRaft 3-broker cluster, Spring Kafka platform, topics via `KafkaAdmin`/`TopicBuilder`, Order produce API | **DONE** |
| 02 | Avro custom key serde, Schema Registry, assignors (Range/Sticky/RR/Cooperative/Custom), offsets, dedup, threading, failure sims | **DONE** |
| 03 | Producer deep dive (`KafkaTemplate`, interceptors, routing, request-reply) | Pending |
| 04 | Serialization / deserialization (JSON, Avro, Protobuf, EHD) | Pending |
| 05 | Consumer framework (`@KafkaListener`, ack modes, interceptors) | Pending |
| 06 | Consumer groups, rebalance, custom assignor | Pending |
| 07 | Offsets, pull model, backpressure | Pending |
| 08 | Retry + DLQ + **DLQ reprocess API** (`DeadLetterPublishingRecoverer`, replay-service) | **DONE** |
| 09 | Transactions + EOS + Outbox/Inbox | Pending |
| 10 | Dedup (Redis + Postgres idempotency) | Pending |
| 11 | Replay REST API | Pending |
| 12 | Security (SSL/SASL/ACL) | Pending |
| 13 | Monitoring (Micrometer/Prometheus/Grafana) | Pending |
| 14 | CDC Debezium | Pending |
| 15 | Testcontainers / concurrency / failure tests | Pending |

Docs: [Coverage](docs/IMPLEMENTATION_COVERAGE.md) · [Q1–Q100](docs/interview/KAFKA_100_QA_WITH_CODE.md) · **[platform-kafka plug-and-play](docs/PLATFORM_KAFKA.md)** · [Security](docs/security/README.md)

## Quick Start (Module 1)

Use **JDK 21** (avoid Loom EA 25 as the Maven JVM):

```bash
export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"

docker compose up -d
mvn -pl order-service -am spring-boot:run

curl -s localhost:8081/api/v1/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-1' \
  -d '{
    "customerId":"cust-1",
    "currency":"USD",
    "totalAmount":99.99,
    "lines":[{"sku":"SKU-1","quantity":1,"unitPrice":99.99}]
  }'

./scripts/cluster-status.sh
./scripts/failover-demo.sh broker-kill kafka-2
```

Useful Maven commands:

```bash
mvn -DskipTests compile
mvn -pl order-service -am package
mvn -pl order-service -am spring-boot:run
```

## Spring Kafka already wired (Module 1)

- `@EnableKafka`
- `KafkaAdmin` + `NewTopic` / `TopicBuilder`
- `DefaultKafkaProducerFactory` / `DefaultKafkaConsumerFactory`
- `KafkaTemplate`
- `JsonSerializer` / `JsonDeserializer` + `ErrorHandlingDeserializer`
- `ConcurrentKafkaListenerContainerFactory` (record, batch, manual ack)
- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `ExponentialBackOff`
- `RecordInterceptor` (tracing / MDC)
- Actuator + Micrometer Prometheus registry on classpath
