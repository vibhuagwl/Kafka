package com.ecommerce.order.kafka.exception;

/**
 * Exception taxonomy for Kafka failure interviews.
 */
public sealed class KafkaPlatformException extends RuntimeException
        permits ProducerFailureException, ConsumerFailureException, SerializationFailureException,
        BrokerFailureException, RetryExhaustedException, PoisonMessageException {

    private final FailureCategory category;

    public KafkaPlatformException(FailureCategory category, String message, Throwable cause) {
        super(message, cause);
        this.category = category;
    }

    public FailureCategory category() {
        return category;
    }

    public enum FailureCategory {
        PRODUCER,
        CONSUMER,
        SERIALIZATION,
        BROKER,
        LEADER,
        CONTROLLER,
        NETWORK,
        RETRY_EXHAUSTED,
        POISON,
        DUPLICATE,
        OFFSET,
        REBALANCE
    }
}
