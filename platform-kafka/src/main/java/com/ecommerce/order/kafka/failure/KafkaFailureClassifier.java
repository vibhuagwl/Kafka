package com.ecommerce.order.kafka.failure;

import com.ecommerce.order.kafka.exception.BrokerFailureException;
import com.ecommerce.order.kafka.exception.ConsumerFailureException;
import com.ecommerce.order.kafka.exception.KafkaPlatformException;
import com.ecommerce.order.kafka.exception.PoisonMessageException;
import com.ecommerce.order.kafka.exception.ProducerFailureException;
import com.ecommerce.order.kafka.exception.RetryExhaustedException;
import com.ecommerce.order.kafka.exception.SerializationFailureException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

/**
 * Maps low-level Kafka / Spring exceptions to interview-friendly categories.
 *
 * <p>Broker fail / leader fail / network → usually {@link RetriableException}
 * (producer retries, consumer re-fetches). Controller fail → metadata update delays.
 * Poison → non-retriable → DLQ.
 */
public final class KafkaFailureClassifier {

    private KafkaFailureClassifier() {
    }

    public static KafkaPlatformException classify(Throwable raw) {
        Throwable ex = unwrap(raw);
        if (ex instanceof SerializationException || ex instanceof com.fasterxml.jackson.core.JsonProcessingException) {
            return new SerializationFailureException(ex.getMessage(), ex);
        }
        if (ex instanceof NotLeaderOrFollowerException) {
            return new BrokerFailureException("Leader moved / not leader — client will refresh metadata", ex);
        }
        if (ex instanceof UnknownTopicOrPartitionException) {
            return new BrokerFailureException("Unknown topic/partition — metadata or ACL issue", ex);
        }
        if (ex instanceof TimeoutException || ex instanceof DisconnectException) {
            return new BrokerFailureException("Broker/network timeout or disconnect", ex);
        }
        if (ex instanceof RetriableException) {
            return new BrokerFailureException("Retriable broker/client failure: " + ex.getClass().getSimpleName(), ex);
        }
        if (ex instanceof PoisonMessageException p) {
            return p;
        }
        if (ex instanceof IllegalArgumentException) {
            return new PoisonMessageException("Non-retryable business/poison: " + ex.getMessage(), ex);
        }
        if (ex instanceof ListenerExecutionFailedException) {
            return new ConsumerFailureException(ex.getMessage(), ex);
        }
        return new ConsumerFailureException(ex.getMessage(), ex);
    }

    public static boolean isRetryable(Throwable raw) {
        KafkaPlatformException classified = classify(raw);
        return classified.category() == KafkaPlatformException.FailureCategory.BROKER
                || classified.category() == KafkaPlatformException.FailureCategory.NETWORK
                || classified.category() == KafkaPlatformException.FailureCategory.LEADER
                || classified.category() == KafkaPlatformException.FailureCategory.CONTROLLER
                || unwrap(raw) instanceof RetriableException;
    }

    public static RetryExhaustedException exhausted(Throwable cause, int attempts) {
        return new RetryExhaustedException("Retries exhausted after " + attempts + " attempts", cause);
    }

    public static ProducerFailureException producer(Throwable cause) {
        return new ProducerFailureException(cause.getMessage(), cause);
    }

    private static Throwable unwrap(Throwable ex) {
        Throwable cur = ex;
        while (cur.getCause() != null && cur.getCause() != cur
                && (cur instanceof ListenerExecutionFailedException || cur.getClass().getName().contains("UndeclaredThrowable"))) {
            cur = cur.getCause();
        }
        return cur;
    }
}
