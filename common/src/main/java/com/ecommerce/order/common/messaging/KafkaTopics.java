package com.ecommerce.order.common.messaging;

/**
 * Canonical topic names for the e-commerce order pipeline.
 *
 * <p><b>Why Spring Kafka {@code NewTopic}/{@code TopicBuilder}?</b>
 * Declarative topic provisioning via {@code KafkaAdmin} issues
 * {@code CreateTopics} AdminClient RPCs against the controller (KRaft).
 * Native alternative: {@code AdminClient#createTopics} or {@code kafka-topics.sh}.
 *
 * <p>Internals: AdminClient discovers the active controller via metadata,
 * then sends CreateTopics to the controller. Partition leaders are assigned
 * using the replica assignment / rack-aware logic on the controller.
 */
public final class KafkaTopics {

    public static final String ORDERS = "orders";
    public static final String INVENTORY = "inventory";
    public static final String PAYMENT = "payment";
    public static final String SHIPPING = "shipping";
    public static final String NOTIFICATION = "notification";
    public static final String AUDIT = "audit";
    public static final String RETRY = "retry";
    public static final String DEAD_LETTER = "dead-letter";
    public static final String REPLY = "reply-topic";
    public static final String TRANSACTION = "transaction-topic";
    public static final String EVENT_STORE = "event-store";

    private KafkaTopics() {
    }
}
