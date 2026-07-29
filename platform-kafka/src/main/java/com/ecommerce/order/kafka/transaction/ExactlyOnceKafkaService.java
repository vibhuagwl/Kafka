package com.ecommerce.order.kafka.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

/**
 * Generic multi-topic transactional publish (EOS). Topic names are caller-supplied.
 */
public class ExactlyOnceKafkaService {

    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceKafkaService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ExactlyOnceKafkaService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional(transactionManager = "kafkaTransactionManager")
    public void publishAtomically(
            String topicA, String keyA, Object valueA,
            String topicB, String keyB, Object valueB
    ) {
        kafkaTemplate.send(topicA, keyA, valueA);
        kafkaTemplate.send(topicB, keyB, valueB);
        log.info("EOS transactional publish topicA={} topicB={}", topicA, topicB);
    }

    public void publishWithExecuteInTransaction(
            String topicA, String keyA, Object valueA,
            String topicB, String keyB, Object valueB
    ) {
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(topicA, keyA, valueA);
            ops.send(topicB, keyB, valueB);
            return true;
        });
    }
}
