package com.ecommerce.order.kafka.config;

import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableKafka
@EnableConfigurationProperties(PlatformKafkaProperties.class)
@ConditionalOnProperty(prefix = "platform.kafka", name = "auto-create-topics", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfiguration {

    @Bean
    public KafkaAdmin.NewTopics platformDeclaredTopics(PlatformKafkaProperties props) {
        short defaultRf = props.defaultReplicationFactor();
        var topics = props.topics().stream()
                .map(spec -> {
                    short rf = spec.replicas() != null ? spec.replicas() : defaultRf;
                    Map<String, String> configs = new HashMap<>();
                    short minIsr = spec.minInSyncReplicas() != null
                            ? spec.minInSyncReplicas()
                            : props.minInSyncReplicas();
                    configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                            String.valueOf(Math.min(minIsr, rf)));
                    configs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
                    if (spec.cleanupPolicy() != null && !spec.cleanupPolicy().isBlank()) {
                        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, spec.cleanupPolicy());
                    }
                    int partitions = spec.partitions() > 0 ? spec.partitions() : 1;
                    return TopicBuilder.name(spec.name())
                            .partitions(partitions)
                            .replicas(rf)
                            .configs(configs)
                            .build();
                })
                .toArray(org.apache.kafka.clients.admin.NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }
}
