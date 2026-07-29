package com.ecommerce.order.kafka.offset;

/**
 * Offset commit / reset strategies covered in interviews.
 *
 * <p>Broker stores consumer offsets in {@code __consumer_offsets} (internal topic).
 * Commit = Produce to that topic keyed by (group, topic, partition).
 */
public enum OffsetCommitMode {
    /** Container commits after each record (AckMode.RECORD). */
    AUTO_RECORD,
    /** Container commits after each poll batch (AckMode.BATCH). */
    AUTO_BATCH,
    /** Listener calls Acknowledgment.acknowledge(); commit sync/async per container. */
    MANUAL,
    /** acknowledge() triggers immediate commit (AckMode.MANUAL_IMMEDIATE). */
    MANUAL_IMMEDIATE,
    /** App stores offsets in DB/Redis and seeks on assign (exactly-once effect with outbox). */
    EXTERNAL
}

/**
 * auto.offset.reset when no committed offset / out of range.
 */
enum OffsetResetPolicy {
    EARLIEST,
    LATEST,
    NONE
}
