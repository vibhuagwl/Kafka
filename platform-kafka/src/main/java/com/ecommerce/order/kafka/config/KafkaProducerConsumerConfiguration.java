package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.partitioner.StickyHashPartitioner;
import com.ecommerce.order.kafka.publisher.KafkaEventPublisher;
import com.ecommerce.order.kafka.transaction.ExactlyOnceKafkaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@AutoConfiguration
public class KafkaProducerConsumerConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new ParameterNamesModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(
            PlatformKafkaProperties props,
            ObjectMapper kafkaObjectMapper
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, props.producer().acks());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, props.producer().enableIdempotence());
        config.put(ProducerConfig.RETRIES_CONFIG, props.producer().retries());
        config.put(ProducerConfig.LINGER_MS_CONFIG, props.producer().lingerMs());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, props.producer().batchSize());
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, props.producer().bufferMemory());
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                props.producer().maxInFlightRequestsPerConnection());
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, props.producer().deliveryTimeoutMs());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, props.producer().requestTimeoutMs());
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, props.producer().compressionType());
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StickyHashPartitioner.NAME);
        config.put("platform.partitioner.mode", props.partitionerMode());
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        applySecurity(config, props);

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(config, new StringSerializer(),
                        new JsonSerializer<>(kafkaObjectMapper));

        if (props.transactionsEnabled() && StringUtils.hasText(props.transactionalIdPrefix())) {
            factory.setTransactionIdPrefix(props.transactionalIdPrefix());
        }
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaTransactionManager")
    @ConditionalOnProperty(prefix = "platform.kafka", name = "transactions-enabled", havingValue = "true")
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> producerFactory
    ) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ExactlyOnceKafkaService exactlyOnceKafkaService(KafkaTemplate<String, Object> kafkaTemplate) {
        return new ExactlyOnceKafkaService(kafkaTemplate);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(
            PlatformKafkaProperties props,
            ObjectMapper kafkaObjectMapper
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.consumer().autoOffsetReset());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, props.consumer().enableAutoCommit());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, props.consumer().isolationLevel());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.consumer().maxPollRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, props.consumer().maxPollIntervalMs());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, props.consumer().sessionTimeoutMs());
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, props.consumer().heartbeatIntervalMs());
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, props.consumer().fetchMinBytes());
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, props.consumer().fetchMaxWaitMs());
        if (StringUtils.hasText(props.groupInstanceId())) {
            config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, props.groupInstanceId());
        }

        String trusted = props.trustedPackages().stream().collect(Collectors.joining(","));
        config.put(JsonDeserializer.TRUSTED_PACKAGES, trusted);
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        if (StringUtils.hasText(props.valueDefaultType())) {
            config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, props.valueDefaultType());
        }
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        applySecurity(config, props);

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
        props.trustedPackages().forEach(jsonDeserializer::addTrustedPackages);

        return new DefaultKafkaConsumerFactory<>(
                config,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    private static void applySecurity(Map<String, Object> config, PlatformKafkaProperties props) {
        if (props.security() == null || !props.security().enabled()) {
            return;
        }
        config.put(org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, props.security().protocol());
        if (StringUtils.hasText(props.security().saslMechanism())) {
            config.put(org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM, props.security().saslMechanism());
        }
        if (StringUtils.hasText(props.security().saslJaasConfig())) {
            config.put(org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG, props.security().saslJaasConfig());
        }
        if (StringUtils.hasText(props.security().truststoreLocation())) {
            config.put("ssl.truststore.location", props.security().truststoreLocation());
            config.put("ssl.truststore.password", props.security().truststorePassword());
        }
    }
}
