package com.ecommerce.order.platformadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication(scanBasePackages = "com.ecommerce.order")
@EnableKafka
public class PlatformAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(PlatformAdminApplication.class, args);
    }
}
