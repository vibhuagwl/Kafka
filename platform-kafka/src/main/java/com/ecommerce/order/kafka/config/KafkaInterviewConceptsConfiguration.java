package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.assignor.AssignorStrategy;
import com.ecommerce.order.kafka.dedup.IdempotentEventProcessor;
import com.ecommerce.order.kafka.offset.OffsetManagementService;
import com.ecommerce.order.kafka.rebalance.InterviewRebalanceListener;
import com.ecommerce.order.kafka.serialization.avro.CustomAvroKeyDeserializer;
import com.ecommerce.order.kafka.serialization.avro.CustomAvroKeySerializer;
import com.ecommerce.order.kafka.serialization.avro.SchemaRegistryAvroSerdes;
import com.ecommerce.order.kafka.threading.PartitionOrderedExecutor;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
public class KafkaInterviewConceptsConfiguration {

    @Bean
    public IdempotentEventProcessor idempotentEventProcessor(ObjectProvider<StringRedisTemplate> redis) {
        return new IdempotentEventProcessor(redis);
    }

    @Bean(destroyMethod = "close")
    public PartitionOrderedExecutor partitionOrderedExecutor() {
        return new PartitionOrderedExecutor();
    }

    @Bean
    public OffsetManagementService offsetManagementService() {
        return new OffsetManagementService();
    }

    @Bean
    public InterviewRebalanceListener interviewRebalanceListener() {
        return new InterviewRebalanceListener();
    }

    @Bean
    @ConditionalOnProperty(prefix = "platform.kafka", name = "schema-registry-url")
    public ProducerFactory<Object, Object> avroProducerFactory(PlatformKafkaProperties props) {
        Map<String, Object> config = baseProducer(props);
        config.putAll(SchemaRegistryAvroSerdes.schemaRegistryConfigs(props.schemaRegistryUrl()));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CustomAvroKeySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    @ConditionalOnProperty(prefix = "platform.kafka", name = "schema-registry-url")
    public KafkaTemplate<Object, Object> avroKafkaTemplate(ProducerFactory<Object, Object> avroProducerFactory) {
        KafkaTemplate<Object, Object> template = new KafkaTemplate<>(avroProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    @ConditionalOnProperty(prefix = "platform.kafka", name = "schema-registry-url")
    public ConsumerFactory<String, Object> avroConsumerFactory(PlatformKafkaProperties props) {
        Map<String, Object> config = baseConsumer(props);
        config.putAll(SchemaRegistryAvroSerdes.schemaRegistryConfigs(props.schemaRegistryUrl()));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, CustomAvroKeyDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, props.assignor().className());

        CustomAvroKeyDeserializer keyDeserializer = new CustomAvroKeyDeserializer();
        keyDeserializer.configure(config, true);

        return new DefaultKafkaConsumerFactory<>(
                config,
                new ErrorHandlingDeserializer<>(keyDeserializer),
                new ErrorHandlingDeserializer<>(SchemaRegistryAvroSerdes.valueDeserializer(config))
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> rangeAssignorListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        return buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener, AssignorStrategy.RANGE);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> stickyAssignorListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        return buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener, AssignorStrategy.STICKY);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> cooperativeStickyListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        return buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener,
                AssignorStrategy.COOPERATIVE_STICKY);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> roundRobinAssignorListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        return buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener,
                AssignorStrategy.ROUND_ROBIN);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> orderAffinityAssignorListenerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        return buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener,
                AssignorStrategy.ORDER_AFFINITY);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> manualImmediateOffsetFactory(
            ConsumerFactory<String, Object> consumerFactory,
            PlatformKafkaProperties props,
            DefaultErrorHandler defaultErrorHandler,
            InterviewRebalanceListener rebalanceListener
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                buildAssignorFactory(consumerFactory, props, defaultErrorHandler, rebalanceListener,
                        AssignorStrategy.COOPERATIVE_STICKY);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        return factory;
    }

    private ConcurrentKafkaListenerContainerFactory<String, Object> buildAssignorFactory(
            ConsumerFactory<String, Object> base,
            PlatformKafkaProperties props,
            DefaultErrorHandler errorHandler,
            InterviewRebalanceListener rebalanceListener,
            AssignorStrategy strategy
    ) {
        Map<String, Object> cfg = new HashMap<>(base.getConfigurationProperties());
        cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, strategy.className());
        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        props.trustedPackages().forEach(jsonDeserializer::addTrustedPackages);
        DefaultKafkaConsumerFactory<String, Object> cf = new DefaultKafkaConsumerFactory<>(
                cfg,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(props.consumer().concurrency());
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    private static Map<String, Object> baseProducer(PlatformKafkaProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, props.producer().acks());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, props.producer().enableIdempotence());
        config.put(ProducerConfig.RETRIES_CONFIG, props.producer().retries());
        config.put(ProducerConfig.LINGER_MS_CONFIG, props.producer().lingerMs());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, props.producer().batchSize());
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, props.producer().compressionType());
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                props.producer().maxInFlightRequestsPerConnection());
        return config;
    }

    private static Map<String, Object> baseConsumer(PlatformKafkaProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.consumer().autoOffsetReset());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, props.consumer().isolationLevel());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.consumer().maxPollRecords());
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.schemaRegistryUrl());
        return config;
    }
}
