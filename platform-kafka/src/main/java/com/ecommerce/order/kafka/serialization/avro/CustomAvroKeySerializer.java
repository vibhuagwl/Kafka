package com.ecommerce.order.kafka.serialization.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic Avro key serializer — accepts {@link GenericRecord} or {@link String}
 * (String wrapped as Avro {@code {"type":"record","name":"Key","fields":[{"name":"id","type":"string"}]}}).
 */
public class CustomAvroKeySerializer implements Serializer<Object> {

    private static final Schema STRING_KEY_SCHEMA = new Schema.Parser().parse("""
            {"type":"record","name":"StringKey","namespace":"com.platform.kafka",
             "fields":[{"name":"id","type":"string"}]}
            """);

    private KafkaAvroSerializer delegate;
    private boolean useSchemaRegistry;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object url = configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        useSchemaRegistry = url != null && !url.toString().isBlank();
        if (useSchemaRegistry) {
            delegate = new KafkaAvroSerializer();
            delegate.configure(new HashMap<>(configs), true);
        }
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return serialize(topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        if (data == null) {
            return null;
        }
        try {
            if (!useSchemaRegistry || delegate == null) {
                return String.valueOf(data).getBytes(StandardCharsets.UTF_8);
            }
            return delegate.serialize(topic, headers, toRecord(data));
        } catch (RuntimeException ex) {
            throw new SerializationException("Failed to serialize Avro key for topic=" + topic, ex);
        }
    }

    private static GenericRecord toRecord(Object data) {
        if (data instanceof GenericRecord gr) {
            return gr;
        }
        GenericRecord record = new GenericData.Record(STRING_KEY_SCHEMA);
        record.put("id", String.valueOf(data));
        return record;
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
