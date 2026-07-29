package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.assignor.AssignorStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Plug-and-play configuration for {@code platform-kafka}.
 *
 * <pre>
 * platform:
 *   kafka:
 *     bootstrap-servers: localhost:9092
 *     dead-letter-topic: dead-letter
 *     topics:
 *       - name: orders
 *         partitions: 6
 *         replicas: 3
 * </pre>
 */
@ConfigurationProperties(prefix = "platform.kafka")
public record PlatformKafkaProperties(
        String bootstrapServers,
        String schemaRegistryUrl,
        AssignorStrategy assignor,
        String transactionalIdPrefix,
        boolean transactionsEnabled,
        String partitionerMode,
        String groupInstanceId,
        String groupIdPrefix,
        String deadLetterTopic,
        String defaultOriginalTopicFallback,
        List<String> retryIncludeTopics,
        List<String> trustedPackages,
        String valueDefaultType,
        short defaultReplicationFactor,
        short minInSyncReplicas,
        Security security,
        Producer producer,
        Consumer consumer,
        List<TopicSpec> topics
) {
    public PlatformKafkaProperties {
        if (assignor == null) {
            assignor = AssignorStrategy.COOPERATIVE_STICKY;
        }
        if (partitionerMode == null || partitionerMode.isBlank()) {
            partitionerMode = "HASH_OR_STICKY";
        }
        if (deadLetterTopic == null || deadLetterTopic.isBlank()) {
            deadLetterTopic = "dead-letter";
        }
        if (defaultOriginalTopicFallback == null) {
            defaultOriginalTopicFallback = "unknown";
        }
        if (retryIncludeTopics == null) {
            retryIncludeTopics = List.of();
        }
        if (trustedPackages == null || trustedPackages.isEmpty()) {
            trustedPackages = List.of("*");
        }
        if (security == null) {
            security = new Security(false, "PLAINTEXT", null, null, null, null);
        }
        if (topics == null) {
            topics = new ArrayList<>();
        }
        if (defaultReplicationFactor == 0) {
            defaultReplicationFactor = 1;
        }
        if (minInSyncReplicas == 0) {
            minInSyncReplicas = 1;
        }
    }

    public record Security(
            boolean enabled,
            String protocol,
            String saslMechanism,
            String saslJaasConfig,
            String truststoreLocation,
            String truststorePassword
    ) {
    }

    public record Producer(
            String acks,
            boolean enableIdempotence,
            int retries,
            int lingerMs,
            int batchSize,
            long bufferMemory,
            int maxInFlightRequestsPerConnection,
            long deliveryTimeoutMs,
            long requestTimeoutMs,
            String compressionType,
            String clientIdPrefix
    ) {
    }

    public record Consumer(
            String autoOffsetReset,
            boolean enableAutoCommit,
            String isolationLevel,
            int maxPollRecords,
            int maxPollIntervalMs,
            int sessionTimeoutMs,
            int heartbeatIntervalMs,
            int fetchMinBytes,
            int fetchMaxWaitMs,
            int concurrency,
            String clientIdPrefix
    ) {
    }

    /**
     * Declarative topic — created by KafkaAdmin when {@code auto-create-topics=true}.
     */
    public record TopicSpec(
            String name,
            int partitions,
            Short replicas,
            String cleanupPolicy,
            Short minInSyncReplicas
    ) {
    }
}
