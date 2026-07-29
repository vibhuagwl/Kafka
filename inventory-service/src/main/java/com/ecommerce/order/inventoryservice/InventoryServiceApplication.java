package com.ecommerce.order.inventoryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.ecommerce.order")
@EnableKafka
@EnableScheduling
public class InventoryServiceApplication extends RetryTopicConfigurationSupport {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
