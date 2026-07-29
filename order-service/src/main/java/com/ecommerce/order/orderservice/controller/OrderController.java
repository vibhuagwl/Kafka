package com.ecommerce.order.orderservice.controller;

import com.ecommerce.order.common.event.DomainEvent;
import com.ecommerce.order.common.event.EventTypes;
import com.ecommerce.order.common.event.OrderPlacedPayload;
import com.ecommerce.order.common.messaging.KafkaTopics;
import com.ecommerce.order.ecommerce.kafka.OrderEventPublisher;
import com.ecommerce.order.orderservice.outbox.OutboxOrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/v1/orders")
@Validated
public class OrderController {

    private final OrderEventPublisher publisher;
    private final OutboxOrderService outboxOrderService;

    public OrderController(OrderEventPublisher publisher, OutboxOrderService outboxOrderService) {
        this.publisher = publisher;
        this.outboxOrderService = outboxOrderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PlaceOrderResponse placeOrder(
            @Valid @RequestBody PlaceOrderRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId
    ) {
        String orderId = UUID.randomUUID().toString();
        String corr = correlationId != null ? correlationId : UUID.randomUUID().toString();
        DomainEvent<OrderPlacedPayload> event = buildEvent(orderId, corr, request);
        SendResult<String, Object> result = publisher.publishOrderPlaced(event);
        return new PlaceOrderResponse(
                orderId, corr,
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset()
        );
    }

    @PostMapping("/outbox")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PlaceOrderResponse placeOrderOutbox(
            @Valid @RequestBody PlaceOrderRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId
    ) {
        String orderId = UUID.randomUUID().toString();
        String corr = correlationId != null ? correlationId : UUID.randomUUID().toString();
        OrderPlacedPayload payload = payload(orderId, request);
        outboxOrderService.placeOrderViaOutbox(payload, corr);
        return new PlaceOrderResponse(orderId, corr, KafkaTopics.ORDERS, -1, -1);
    }

    private static DomainEvent<OrderPlacedPayload> buildEvent(
            String orderId, String corr, PlaceOrderRequest request
    ) {
        return DomainEvent.of(EventTypes.ORDER_PLACED, "Order", orderId, corr, payload(orderId, request));
    }

    private static OrderPlacedPayload payload(String orderId, PlaceOrderRequest request) {
        return new OrderPlacedPayload(
                orderId,
                request.customerId(),
                request.lines().stream()
                        .map(l -> new OrderPlacedPayload.OrderLine(l.sku(), l.quantity(), l.unitPrice()))
                        .toList(),
                request.totalAmount(),
                request.currency()
        );
    }

    public record PlaceOrderRequest(
            @NotBlank String customerId,
            @NotEmpty List<Line> lines,
            @NotNull BigDecimal totalAmount,
            @NotBlank String currency
    ) {
        public record Line(@NotBlank String sku, int quantity, @NotNull BigDecimal unitPrice) {
        }
    }

    public record PlaceOrderResponse(
            String orderId, String correlationId, String topic, int partition, long offset
    ) {
    }
}
