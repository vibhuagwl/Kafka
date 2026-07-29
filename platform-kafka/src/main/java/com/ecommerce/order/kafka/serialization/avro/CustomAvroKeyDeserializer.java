package com.ecommerce.order.kafka.serialization.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Generic Avro key deserializer — returns string id from record or UTF-8 fallback. */
public class CustomAvroKeyDeserializer implements Deserializer<String> {

    private static final Logger log = LoggerFactory.getLogger(CustomAvroKeyDeserializer.class);
    private static final byte MAGIC = 0x0;

    private KafkaAvroDeserializer delegate;
    private boolean useSchemaRegistry = true;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object url = configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        useSchemaRegistry = url != null && !url.toString().isBlank();
        if (useSchemaRegistry) {
            Map<String, Object> copy = new HashMap<>(configs);
            copy.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
            delegate = new KafkaAvroDeserializer();
            delegate.configure(copy, true);
        }
        log.info("CustomAvroKeyDeserializer schemaRegistry={}", useSchemaRegistry);
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            if (useSchemaRegistry && delegate != null && data.length > 5 && data[0] == MAGIC) {
                Object raw = delegate.deserialize(topic, headers, data);
                if (raw instanceof GenericRecord record) {
                    Object id = record.get("id");
                    if (id == null) {
                        id = record.get("orderId");
                    }
                    return id == null ? record.toString() : id.toString();
                }
                return String.valueOf(raw);
            }
            return new String(data, StandardCharsets.UTF_8);
        } catch (RuntimeException ex) {
            throw new SerializationException("Failed to deserialize Avro key topic=" + topic, ex);
        }
    }

    public static int peekSchemaId(byte[] data) {
        if (data == null || data.length <= 5 || data[0] != MAGIC) {
            return -1;
        }
        return ByteBuffer.wrap(data, 1, 4).getInt();
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
