package com.ecommerce.order.inventoryservice.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Inbox pattern (Q98) — persist processed event ids for durable idempotency.
 */
@Entity
@Table(name = "inbox_event")
public class InboxEventEntity {

    @Id
    @Column(length = 64)
    private String eventId;

    @Column(nullable = false)
    private String consumerName;

    @Column(nullable = false)
    private Instant processedAt = Instant.now();

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getConsumerName() { return consumerName; }
    public void setConsumerName(String consumerName) { this.consumerName = consumerName; }
    public Instant getProcessedAt() { return processedAt; }
}
