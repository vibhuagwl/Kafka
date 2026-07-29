package com.ecommerce.order.kafka.assignor;

/**
 * Partition assignment strategies used in FAANG interviews.
 *
 * <p>Configured via {@code partition.assignment.strategy} on the consumer.
 * Spring Kafka passes this through {@code ConsumerFactory} → {@code KafkaConsumer}.
 *
 * <p>On JoinGroup, each member advertises supported assignors; group coordinator
 * picks one (prefer first mutually supported) and runs assignment in leader
 * (eager) or cooperative protocol.
 */
public enum AssignorStrategy {

    /** Classic: contiguous partition ranges per member. Uneven if partitions % members != 0. */
    RANGE("org.apache.kafka.clients.consumer.RangeAssignor"),

    /** Round-robin partitions across members. Better balance, may move more on rebalance. */
    ROUND_ROBIN("org.apache.kafka.clients.consumer.RoundRobinAssignor"),

    /** Minimize partition movement on rebalance (sticky). Eager protocol. */
    STICKY("org.apache.kafka.clients.consumer.StickyAssignor"),

    /** Sticky + cooperative: incremental revoke, less stop-the-world. Default in modern clients. */
    COOPERATIVE_STICKY("org.apache.kafka.clients.consumer.CooperativeStickyAssignor"),

    /** Custom: keep all partitions for same customer-hash bucket on same member when possible. */
    ORDER_AFFINITY("com.ecommerce.order.kafka.assignor.OrderAffinityAssignor");

    private final String className;

    AssignorStrategy(String className) {
        this.className = className;
    }

    public String className() {
        return className;
    }
}
