package com.ecommerce.order.kafka.exception;

public final class PoisonMessageException extends KafkaPlatformException {
    public PoisonMessageException(String message, Throwable cause) {
        super(FailureCategory.POISON, message, cause);
    }
}
