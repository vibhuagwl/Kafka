package com.ecommerce.order.orderservice.outbox;

import com.ecommerce.order.common.event.DomainEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Polling outbox publisher (Q85). Debezium can replace this by streaming outbox_event CDC.
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPoller(
            OutboxEventRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${ecommerce.outbox.poll-ms:1000}")
    @Transactional
    public void publishNewEvents() {
        List<OutboxEventEntity> batch = repository.lockBatchForPublish();
        for (OutboxEventEntity row : batch) {
            try {
                DomainEvent<?> event = objectMapper.readValue(row.getPayloadJson(), DomainEvent.class);
                kafkaTemplate.send(row.getTopic(), row.getPartitionKey(), event).get();
                row.setStatus(OutboxEventEntity.Status.PUBLISHED);
                row.setPublishedAt(Instant.now());
            } catch (Exception ex) {
                log.error("Outbox publish failed id={} eventId={}", row.getId(), row.getEventId(), ex);
                row.setStatus(OutboxEventEntity.Status.FAILED);
            }
        }
    }
}
