package com.ecommerce.order.kafka.exception;

public final class SerializationFailureException extends KafkaPlatformException {
    public SerializationFailureException(String message, Throwable cause) {
        super(FailureCategory.SERIALIZATION, message, cause);
    }
}
