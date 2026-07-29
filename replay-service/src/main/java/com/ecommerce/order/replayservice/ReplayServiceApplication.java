package com.ecommerce.order.replayservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication(scanBasePackages = "com.ecommerce.order")
@EnableKafka
public class ReplayServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReplayServiceApplication.class, args);
    }
}
