package com.ecommerce.order.kafka.config;

import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

public class TracingRecordInterceptor implements RecordInterceptor<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(TracingRecordInterceptor.class);

    @Override
    public ConsumerRecord<String, Object> intercept(
            ConsumerRecord<String, Object> record,
            Consumer<String, Object> consumer
    ) {
        headerAsString(record, PlatformHeaders.CORRELATION_ID)
                .ifPresent(v -> MDC.put(PlatformHeaders.CORRELATION_ID, v));
        headerAsString(record, PlatformHeaders.TRACE_ID)
                .ifPresent(v -> MDC.put(PlatformHeaders.TRACE_ID, v));
        headerAsString(record, PlatformHeaders.EVENT_ID)
                .ifPresent(v -> MDC.put(PlatformHeaders.EVENT_ID, v));
        log.debug("intercept topic={} partition={} offset={}",
                record.topic(), record.partition(), record.offset());
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<String, Object> record, Consumer<String, Object> consumer) {
        MDC.remove(PlatformHeaders.CORRELATION_ID);
        MDC.remove(PlatformHeaders.TRACE_ID);
        MDC.remove(PlatformHeaders.EVENT_ID);
    }

    private static java.util.Optional<String> headerAsString(ConsumerRecord<String, Object> record, String header) {
        var h = record.headers().lastHeader(header);
        if (h == null) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(new String(h.value(), java.nio.charset.StandardCharsets.UTF_8));
    }
}
