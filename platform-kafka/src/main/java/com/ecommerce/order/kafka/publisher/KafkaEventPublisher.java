package com.ecommerce.order.kafka.publisher;

import com.ecommerce.order.kafka.failure.KafkaFailureClassifier;
import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Domain-agnostic publisher — any payload type, optional headers map.
 */
public class KafkaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Object>> publishAsync(String topic, String key, Object value) {
        return publishAsync(topic, key, value, Map.of());
    }

    public CompletableFuture<SendResult<String, Object>> publishAsync(
            String topic,
            String key,
            Object value,
            Map<String, String> headers
    ) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        headers.forEach((k, v) -> addHeader(record, k, v));
        log.info("publishAsync topic={} key={}", topic, key);
        return kafkaTemplate.send(record).whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("PRODUCER FAIL topic={} key={}", topic, key, ex);
            }
        });
    }

    public SendResult<String, Object> publishSync(String topic, String key, Object value) {
        return publishSync(topic, key, value, Map.of());
    }

    public SendResult<String, Object> publishSync(
            String topic,
            String key,
            Object value,
            Map<String, String> headers
    ) {
        try {
            return publishAsync(topic, key, value, headers).get();
        } catch (Exception ex) {
            throw KafkaFailureClassifier.producer(ex);
        }
    }

    public void publishFireAndForget(String topic, String key, Object value) {
        publishAsync(topic, key, value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("fire-and-forget failed topic={} key={}", topic, key, ex);
            }
        });
    }

    public void publishInTransaction(String topic, String key, Object value) {
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(topic, key, value);
            return true;
        });
    }

    public KafkaTemplate<String, Object> template() {
        return kafkaTemplate;
    }

    private static void addHeader(ProducerRecord<String, Object> record, String key, String value) {
        if (value == null) {
            return;
        }
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    /** Convenience for idempotent event publishing. */
    public static Map<String, String> eventHeaders(String eventId, String eventType, String correlationId) {
        return Map.of(
                PlatformHeaders.EVENT_ID, eventId,
                PlatformHeaders.EVENT_TYPE, eventType,
                PlatformHeaders.CORRELATION_ID, correlationId == null ? "" : correlationId,
                PlatformHeaders.IDEMPOTENCY_KEY, eventId
        );
    }
}
