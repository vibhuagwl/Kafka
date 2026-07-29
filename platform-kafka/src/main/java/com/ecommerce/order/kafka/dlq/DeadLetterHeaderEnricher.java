package com.ecommerce.order.kafka.dlq;

import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.support.KafkaHeaders;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class DeadLetterHeaderEnricher {

    private DeadLetterHeaderEnricher() {
    }

    public static void enrich(
            ProducerRecord<Object, Object> dltRecord,
            ConsumerRecord<?, ?> original,
            Exception exception,
            String fallbackOriginalTopic
    ) {
        Headers headers = dltRecord.headers();
        put(headers, PlatformHeaders.ORIGINAL_TOPIC, original.topic());
        put(headers, PlatformHeaders.ORIGINAL_PARTITION, String.valueOf(original.partition()));
        put(headers, PlatformHeaders.ORIGINAL_OFFSET, String.valueOf(original.offset()));
        put(headers, PlatformHeaders.FAILURE_REASON, safeMessage(exception));
        put(headers, PlatformHeaders.EXCEPTION_FQCN, exception.getClass().getName());
        put(headers, PlatformHeaders.EXCEPTION_STACKTRACE, stackTrace(exception));
        put(headers, PlatformHeaders.RETRY_COUNT, header(original.headers(), PlatformHeaders.RETRY_COUNT).orElse("0"));
        put(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC, original.topic());
        put(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(original.partition()));
        put(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(original.offset()));
        put(headers, KafkaHeaders.DLT_EXCEPTION_FQCN, exception.getClass().getName());
        put(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE, safeMessage(exception));
        // silence unused
        if (fallbackOriginalTopic == null) {
            // no-op
        }
    }

    public static Optional<String> header(Headers headers, String key) {
        Header h = headers.lastHeader(key);
        if (h == null || h.value() == null) {
            return Optional.empty();
        }
        return Optional.of(new String(h.value(), StandardCharsets.UTF_8));
    }

    public static String resolveOriginalTopic(ConsumerRecord<?, ?> dlqRecord, String fallback) {
        return header(dlqRecord.headers(), PlatformHeaders.ORIGINAL_TOPIC)
                .or(() -> header(dlqRecord.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC))
                .orElse(fallback);
    }

    static void put(Headers headers, String key, String value) {
        if (value == null) {
            return;
        }
        headers.remove(key);
        headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    public static Headers copyHeaders(Headers source) {
        RecordHeaders copy = new RecordHeaders();
        source.forEach(copy::add);
        return copy;
    }

    private static String safeMessage(Exception ex) {
        String msg = ex.getMessage();
        return msg == null ? ex.getClass().getSimpleName() : msg.substring(0, Math.min(msg.length(), 2000));
    }

    private static String stackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String s = sw.toString();
        return s.substring(0, Math.min(s.length(), 8000));
    }
}
