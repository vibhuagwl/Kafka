# Security (Q73–Q78)

## Implemented in code
Spring clients read `ecommerce.kafka.security.*` and set:
- `security.protocol` (SSL / SASL_SSL / SASL_PLAINTEXT)
- `sasl.mechanism` (SCRAM-SHA-256 / PLAIN / OAUTHBEARER)
- `sasl.jaas.config`
- truststore location/password for TLS

See `KafkaProducerConsumerConfiguration.applySecurity`.

## Local default
Compose uses **PLAINTEXT** for learning (no friction). Overlay:
```bash
docker compose -f docker-compose.yml -f docker-compose.security.yml config
```

## Production checklist
1. Generate broker/client certs (TLS) — encryption in transit (Q73)
2. SCRAM users (Q75) or OAuth (Q76)
3. Enable `StandardAuthorizer` + ACLs (Q77): topic write/read, group, transactional-id
4. Disk encryption / cloud volume KMS for encryption at rest (Q78) — outside Kafka process
5. Rotate secrets; never commit JAAS passwords

## Example Spring config
```yaml
ecommerce:
  kafka:
    security:
      enabled: true
      protocol: SASL_SSL
      sasl-mechanism: SCRAM-SHA-256
      sasl-jaas-config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="order-service" password="${KAFKA_PASSWORD}";
      truststore-location: /certs/truststore.jks
      truststore-password: ${TRUSTSTORE_PASSWORD}
```
