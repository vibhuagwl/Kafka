package com.ecommerce.order.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Order Service entrypoint.
 *
 * <p>{@link EnableKafka} activates Spring Kafka annotation processing
 * ({@code @KafkaListener}, listener containers). Topic beans from
 * {@code platform-kafka} are applied via {@code KafkaAdmin} on startup.
 */
@SpringBootApplication(scanBasePackages = "com.ecommerce.order")
@EnableKafka
@EnableScheduling
@EnableTransactionManagement
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
