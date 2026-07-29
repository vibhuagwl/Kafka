package com.ecommerce.order.kafka.dlq;

public class DlqReprocessException extends RuntimeException {

    public DlqReprocessException(String message) {
        super(message);
    }

    public DlqReprocessException(String message, Throwable cause) {
        super(message, cause);
    }
}
