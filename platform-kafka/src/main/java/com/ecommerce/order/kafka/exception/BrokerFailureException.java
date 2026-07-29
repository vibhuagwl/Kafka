package com.ecommerce.order.kafka.exception;

public final class BrokerFailureException extends KafkaPlatformException {
    public BrokerFailureException(String message, Throwable cause) {
        super(FailureCategory.BROKER, message, cause);
    }
}
