# platform-kafka — Plug-and-Play Starter

Domain-agnostic Spring Boot auto-configuration for Kafka.

## Depend

```xml
<dependency>
  <groupId>com.ecommerce.order</groupId>
  <artifactId>platform-kafka</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Configure (any app)

```yaml
spring:
  config:
    import: optional:classpath:application-kafka.yml

platform:
  kafka:
    bootstrap-servers: localhost:9092
    dead-letter-topic: my-dlt
    trusted-packages: ["com.mycompany.*"]
    topics:
      - name: payments
        partitions: 12
        replicas: 3
```

## You get automatically

| Feature | Bean / behavior |
|---------|-----------------|
| Producer/Consumer | `KafkaTemplate`, factories, `StickyHashPartitioner` |
| Topics | From `platform.kafka.topics[*]` via `KafkaAdmin` |
| DLQ | Configurable topic + reprocess service |
| Assignors | Range / Sticky / Cooperative / custom factories |
| Dedup / threading / offsets | Ready beans |
| Security | `platform.kafka.security.*` |
| Transactions | `platform.kafka.transactions-enabled=true` |
| Avro | Optional when `schema-registry-url` set |
| Retry topics | When `retry-topics-enabled=true` + `retry-include-topics` |

## What it does **not** include

No ecommerce topic names, no `DomainEvent`, no order Avro schemas.

For this monorepo’s order domain, add:

```xml
<dependency>
  <artifactId>ecommerce-kafka</artifactId>
</dependency>
```

and import `application-ecommerce-kafka.yml`.

## Publish from any service

```java
@Autowired KafkaEventPublisher publisher;

publisher.publishSync("payments", paymentId, payload, Map.of("X-Event-Id", id));
```
