package com.ecommerce.order.kafka.retry;

import com.ecommerce.order.kafka.config.PlatformKafkaProperties;
import com.ecommerce.order.kafka.exception.PoisonMessageException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(RetryTopicConfiguration.class)
@ConditionalOnProperty(prefix = "platform.kafka", name = "retry-topics-enabled", havingValue = "true")
public class SpringRetryTopicConfiguration {

    @Bean
    public RetryTopicConfiguration platformRetryTopicConfiguration(
            KafkaTemplate<?, ?> template,
            PlatformKafkaProperties props
    ) {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> typed = (KafkaTemplate<String, Object>) template;
        List<String> topics = props.retryIncludeTopics();
        if (topics == null || topics.isEmpty()) {
            return RetryTopicConfigurationBuilder.newInstance()
                    .exponentialBackoff(1_000, 2.0, 30_000)
                    .maxAttempts(4)
                    .create(typed);
        }
        return RetryTopicConfigurationBuilder
                .newInstance()
                .exponentialBackoff(1_000, 2.0, 30_000)
                .maxAttempts(4)
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .notRetryOn(List.of(IllegalArgumentException.class, PoisonMessageException.class))
                .sameIntervalTopicReuseStrategy(SameIntervalTopicReuseStrategy.SINGLE_TOPIC)
                .includeTopics(topics)
                .create(typed);
    }
}
