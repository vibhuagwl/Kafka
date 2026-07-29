package com.ecommerce.order.ecommerce.kafka;

import com.ecommerce.order.common.event.DomainEvent;
import com.ecommerce.order.common.event.OrderPlacedPayload;
import com.ecommerce.order.common.messaging.KafkaTopics;
import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import com.ecommerce.order.kafka.publisher.KafkaEventPublisher;
import org.springframework.kafka.support.SendResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Domain helper on top of generic {@link KafkaEventPublisher}.
 */
public class OrderEventPublisher {

    private final KafkaEventPublisher publisher;

    public OrderEventPublisher(KafkaEventPublisher publisher) {
        this.publisher = publisher;
    }

    public <T> SendResult<String, Object> publishSync(String topic, DomainEvent<T> event) {
        return publisher.publishSync(topic, event.aggregateId(), event, headersOf(event));
    }

    public <T> CompletableFuture<SendResult<String, Object>> publishAsync(String topic, DomainEvent<T> event) {
        return publisher.publishAsync(topic, event.aggregateId(), event, headersOf(event));
    }

    public SendResult<String, Object> publishOrderPlaced(DomainEvent<OrderPlacedPayload> event) {
        return publishSync(KafkaTopics.ORDERS, event);
    }

    private static <T> Map<String, String> headersOf(DomainEvent<T> event) {
        Map<String, String> headers = new HashMap<>();
        headers.put(PlatformHeaders.EVENT_ID, event.eventId().toString());
        headers.put(PlatformHeaders.EVENT_TYPE, event.eventType());
        headers.put(PlatformHeaders.CORRELATION_ID, event.correlationId() == null ? "" : event.correlationId());
        headers.put(PlatformHeaders.IDEMPOTENCY_KEY, event.eventId().toString());
        headers.put(PlatformHeaders.MESSAGE_VERSION, String.valueOf(event.messageVersion()));
        headers.put(PlatformHeaders.SCHEMA_VERSION, String.valueOf(event.schemaVersion()));
        if (event.causationId() != null) {
            headers.put(PlatformHeaders.CAUSATION_ID, event.causationId());
        }
        return headers;
    }
}
