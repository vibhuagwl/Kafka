package com.ecommerce.order.orderservice.outbox;

import com.ecommerce.order.common.event.DomainEvent;
import com.ecommerce.order.common.event.EventTypes;
import com.ecommerce.order.common.event.OrderPlacedPayload;
import com.ecommerce.order.common.messaging.KafkaTopics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OutboxOrderService {

    private final OutboxEventRepository outbox;
    private final ObjectMapper objectMapper;

    public OutboxOrderService(OutboxEventRepository outbox, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Business write + outbox row in ONE DB transaction (no Kafka yet).
     */
    @Transactional
    public DomainEvent<OrderPlacedPayload> placeOrderViaOutbox(
            OrderPlacedPayload payload,
            String correlationId
    ) {
        DomainEvent<OrderPlacedPayload> event = DomainEvent.of(
                EventTypes.ORDER_PLACED,
                "Order",
                payload.orderId(),
                correlationId,
                payload
        );

        OutboxEventEntity row = new OutboxEventEntity();
        row.setEventId(event.eventId());
        row.setAggregateType("Order");
        row.setAggregateId(payload.orderId());
        row.setEventType(EventTypes.ORDER_PLACED);
        row.setTopic(KafkaTopics.ORDERS);
        row.setPartitionKey(payload.orderId());
        try {
            row.setPayloadJson(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        outbox.save(row);
        return event;
    }
}
