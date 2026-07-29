package com.ecommerce.order.kafka.exception;

public final class ConsumerFailureException extends KafkaPlatformException {
    public ConsumerFailureException(String message, Throwable cause) {
        super(FailureCategory.CONSUMER, message, cause);
    }
}
