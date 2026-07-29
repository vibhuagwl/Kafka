package com.ecommerce.order.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class StreamsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsServiceApplication.class, args);
    }
}
