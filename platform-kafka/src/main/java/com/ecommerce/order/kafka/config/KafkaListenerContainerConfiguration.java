package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.config.PlatformKafkaProperties;
import com.ecommerce.order.kafka.dlq.DeadLetterHeaderEnricher;
import com.ecommerce.order.kafka.dlq.DeadLetterInspectionService;
import com.ecommerce.order.kafka.dlq.DeadLetterReprocessService;
import com.ecommerce.order.kafka.exception.PoisonMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@AutoConfiguration
public class KafkaListenerContainerConfiguration {

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, Object> kafkaTemplate,
            PlatformKafkaProperties props
    ) {
        String dlt = props.deadLetterTopic();
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) ->
                        new TopicPartition(dlt, record.partition())
        );
        recoverer.setHeadersFunction((record, ex) -> {
            RecordHeaders headers = new RecordHeaders();
            ProducerRecord<Object, Object> tmp = new ProducerRecord<>(
                    dlt, record.partition(), record.key(), record.value(), headers);
            DeadLetterHeaderEnricher.enrich(tmp, record, ex, props.defaultOriginalTopicFallback());
            return tmp.headers();
        });
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxElapsedTime(30_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class, PoisonMessageException.class);
        return handler;
    }

    @Bean
    public DeadLetterReprocessService deadLetterReprocessService(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            PlatformKafkaProperties props,
            org.springframework.beans.factory.ObjectProvider<com.ecommerce.order.kafka.dedup.IdempotentEventProcessor> idempotent
    ) {
        return new DeadLetterReprocessService(consumerFactory, kafkaTemplate, props.deadLetterTopic(),
                props.defaultOriginalTopicFallback(), idempotent);
    }

    @Bean
    public DeadLetterInspectionService deadLetterInspectionService(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props
    ) {
        return new DeadLetterInspectionService(consumerFactory, props.deadLetterTopic(),
                props.defaultOriginalTopicFallback());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.consumer().concurrency());
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setRecordInterceptor(new TracingRecordInterceptor());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> manualAckKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.consumer().concurrency());
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(Math.max(1, props.consumer().concurrency() / 2));
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
