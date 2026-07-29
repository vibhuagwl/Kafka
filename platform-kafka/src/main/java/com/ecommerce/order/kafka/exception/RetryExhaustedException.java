package com.ecommerce.order.kafka.exception;

public final class RetryExhaustedException extends KafkaPlatformException {
    public RetryExhaustedException(String message, Throwable cause) {
        super(FailureCategory.RETRY_EXHAUSTED, message, cause);
    }
}
