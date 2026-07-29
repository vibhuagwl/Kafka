package com.ecommerce.order.common.messaging;

/**
 * Standard Kafka record headers used across all services.
 *
 * <p>Spring Kafka maps these via {@code DefaultKafkaHeaderMapper} /
 * {@code MessagingMessageConverter}. On the wire they are Kafka
 * {@code RecordHeaders} attached to each ProduceRequest record batch.
 */
public final class MessageHeaders {

    public static final String CORRELATION_ID = "X-Correlation-Id";
    public static final String TRACE_ID = "X-Trace-Id";
    public static final String REQUEST_ID = "X-Request-Id";
    public static final String EVENT_ID = "X-Event-Id";
    public static final String EVENT_TYPE = "X-Event-Type";
    public static final String MESSAGE_VERSION = "X-Message-Version";
    public static final String SCHEMA_VERSION = "X-Schema-Version";
    public static final String TTL_MS = "X-TTL-Ms";
    public static final String PRIORITY = "X-Priority";
    public static final String SOURCE_SERVICE = "X-Source-Service";
    public static final String CAUSATION_ID = "X-Causation-Id";
    public static final String IDEMPOTENCY_KEY = "X-Idempotency-Key";
    public static final String REPLY_TOPIC = "kafka_replyTopic";
    public static final String REPLY_PARTITION = "kafka_replyPartition";

    /** DLQ / retry diagnostics (populated by error handlers). */
    public static final String ORIGINAL_TOPIC = "X-Original-Topic";
    public static final String ORIGINAL_PARTITION = "X-Original-Partition";
    public static final String ORIGINAL_OFFSET = "X-Original-Offset";
    public static final String RETRY_COUNT = "X-Retry-Count";
    public static final String FAILURE_REASON = "X-Failure-Reason";
    public static final String EXCEPTION_FQCN = "X-Exception-Fqcn";
    public static final String EXCEPTION_STACKTRACE = "X-Exception-Stacktrace";

    /** Set when a DLQ message is republished to its original topic. */
    public static final String REPROCESSED_FROM_DLQ = "X-Reprocessed-From-Dlq";
    public static final String REPROCESS_COUNT = "X-Reprocess-Count";
    public static final String REPROCESS_ID = "X-Reprocess-Id";
    public static final String FORCE_REPROCESS = "X-Force-Reprocess";
    public static final String DLQ_TOPIC = "X-Dlq-Topic";
    public static final String DLQ_PARTITION = "X-Dlq-Partition";
    public static final String DLQ_OFFSET = "X-Dlq-Offset";

    private MessageHeaders() {
    }
}
