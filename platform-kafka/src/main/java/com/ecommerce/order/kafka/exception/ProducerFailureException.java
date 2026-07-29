package com.ecommerce.order.kafka.exception;

public final class ProducerFailureException extends KafkaPlatformException {
    public ProducerFailureException(String message, Throwable cause) {
        super(FailureCategory.PRODUCER, message, cause);
    }
}
