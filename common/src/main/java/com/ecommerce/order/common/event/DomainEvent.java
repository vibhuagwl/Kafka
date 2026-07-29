package com.ecommerce.order.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Envelope for every domain event published to Kafka.
 *
 * <p>Immutable record ensures safe sharing across threads (producer I/O thread,
 * listener container threads, worker pools).
 *
 * <p><b>Partition key:</b> typically {@code aggregateId} (e.g. orderId) so all
 * events for one order land on the same partition and retain total order.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DomainEvent<T>(
        UUID eventId,
        String eventType,
        String aggregateType,
        String aggregateId,
        int messageVersion,
        int schemaVersion,
        Instant occurredAt,
        String causationId,
        String correlationId,
        T payload
) {
    public static <T> DomainEvent<T> of(
            String eventType,
            String aggregateType,
            String aggregateId,
            String correlationId,
            T payload
    ) {
        return new DomainEvent<>(
                UUID.randomUUID(),
                eventType,
                aggregateType,
                aggregateId,
                1,
                1,
                Instant.now(),
                null,
                correlationId,
                payload
        );
    }
}
