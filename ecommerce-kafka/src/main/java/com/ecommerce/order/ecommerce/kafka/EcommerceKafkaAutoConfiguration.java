package com.ecommerce.order.ecommerce.kafka;

import com.ecommerce.order.kafka.publisher.KafkaEventPublisher;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class EcommerceKafkaAutoConfiguration {

    @Bean
    public OrderEventPublisher orderEventPublisher(KafkaEventPublisher publisher) {
        return new OrderEventPublisher(publisher);
    }
}
