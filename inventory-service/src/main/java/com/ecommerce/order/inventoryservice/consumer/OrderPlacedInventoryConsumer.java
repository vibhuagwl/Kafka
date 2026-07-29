package com.ecommerce.order.inventoryservice.consumer;

import com.ecommerce.order.common.event.DomainEvent;
import com.ecommerce.order.common.messaging.KafkaTopics;
import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import com.ecommerce.order.inventoryservice.inbox.InboxService;
import com.ecommerce.order.kafka.dedup.IdempotentEventProcessor;
import com.ecommerce.order.kafka.exception.PoisonMessageException;
import com.ecommerce.order.kafka.failure.KafkaFailureClassifier;
import com.ecommerce.order.kafka.threading.PartitionOrderedExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Inventory consumer: inbox + Redis dedup + RetryableTopic + manual ack + workers.
 */
@Component
public class OrderPlacedInventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacedInventoryConsumer.class);

    private final IdempotentEventProcessor idempotent;
    private final PartitionOrderedExecutor executor;
    private final InboxService inbox;

    public OrderPlacedInventoryConsumer(
            IdempotentEventProcessor idempotent,
            PartitionOrderedExecutor executor,
            InboxService inbox
    ) {
        this.idempotent = idempotent;
        this.executor = executor;
        this.inbox = inbox;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 30000),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "true",
            kafkaTemplate = "kafkaTemplate",
            exclude = {IllegalArgumentException.class, PoisonMessageException.class}
    )
    @KafkaListener(
            topics = KafkaTopics.ORDERS,
            groupId = "${platform.kafka.group-id-prefix}inventory-service",
            containerFactory = "manualImmediateOffsetFactory"
    )
    public void onOrderPlaced(
            ConsumerRecord<String, Object> record,
            @Header(value = PlatformHeaders.EVENT_ID, required = false) String eventId,
            @Header(value = PlatformHeaders.FORCE_REPROCESS, required = false) String forceReprocess,
            Acknowledgment ack
    ) {
        String id = eventId != null ? eventId : extractEventId(record.value());
        boolean force = "true".equalsIgnoreCase(forceReprocess);
        try {
            if (force) {
                idempotent.clear(id);
                inbox.clear(id);
                log.info("Forced DLQ/retry reprocess — cleared idempotency eventId={}", id);
            } else if (!inbox.tryAccept(id, "inventory-service") || idempotent.alreadyProcessed(id)) {
                ack.acknowledge();
                return;
            }

            executor.submitAsync(record.partition(), record, r -> process(r, id)).join();
            idempotent.markProcessed(id);
            ack.acknowledge();
        } catch (Exception ex) {
            var classified = KafkaFailureClassifier.classify(ex);
            log.error("CONSUMER FAIL category={} topic={} partition={} offset={}",
                    classified.category(), record.topic(), record.partition(), record.offset(), ex);
            if (!KafkaFailureClassifier.isRetryable(ex)) {
                throw new PoisonMessageException("Poison inventory message eventId=" + id, ex);
            }
            throw classified;
        }
    }

    private void process(ConsumerRecord<String, Object> record, String eventId) {
        log.info("inventory reserve eventId={} key={} partition={} offset={}",
                eventId, record.key(), record.partition(), record.offset());
        if (record.key() != null && record.key().contains("POISON")) {
            throw new IllegalArgumentException("Simulated poison key");
        }
    }

    private static String extractEventId(Object value) {
        if (value instanceof DomainEvent<?> event) {
            return event.eventId().toString();
        }
        return null;
    }
}
