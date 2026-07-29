package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.publisher.KafkaEventPublisher;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
public class KafkaPublisherConfiguration {

    @Bean
    public KafkaEventPublisher kafkaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        return new KafkaEventPublisher(kafkaTemplate);
    }
}
