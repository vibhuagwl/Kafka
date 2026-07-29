package com.ecommerce.order.kafka.serialization.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Value Avro serdes backed by Schema Registry (BACKWARD compatibility in Compose).
 *
 * <p>Interview: schema evolution — add optional fields with defaults = backward compatible
 * consumers; remove fields carefully = forward/full compatibility discussions.
 */
public final class SchemaRegistryAvroSerdes {

    private SchemaRegistryAvroSerdes() {
    }

    public static Serializer<Object> valueSerializer(Map<String, ?> baseConfigs) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(new HashMap<>(baseConfigs), false);
        return serializer;
    }

    public static Deserializer<Object> valueDeserializer(Map<String, ?> baseConfigs) {
        Map<String, Object> copy = new HashMap<>(baseConfigs);
        copy.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(copy, false);
        return deserializer;
    }

    public static Map<String, Object> schemaRegistryConfigs(String schemaRegistryUrl) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        cfg.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        cfg.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false);
        return cfg;
    }

    /** Wrapper preserving Headers for Spring Kafka 3.x Serializer SPI. */
    public static Serializer<Object> headerAware(Serializer<Object> delegate) {
        return new Serializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                delegate.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                return delegate.serialize(topic, data);
            }

            @Override
            public byte[] serialize(String topic, Headers headers, Object data) {
                return delegate.serialize(topic, headers, data);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
