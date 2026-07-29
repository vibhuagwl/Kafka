package com.ecommerce.order.common.event;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;

/**
 * Order placed payload — published to {@code orders} topic by Order Service.
 */
public record OrderPlacedPayload(
        @NotBlank String orderId,
        @NotBlank String customerId,
        @NotEmpty List<OrderLine> lines,
        @NotNull BigDecimal totalAmount,
        @NotBlank String currency
) {
    public record OrderLine(
            @NotBlank String sku,
            @Min(1) int quantity,
            @NotNull BigDecimal unitPrice
    ) {
    }
}
