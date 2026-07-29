package com.ecommerce.order.kafka.dlq;

import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DeadLetterInspectionService {

    private final ConsumerFactory<String, Object> consumerFactory;
    private final String deadLetterTopic;
    private final String originalTopicFallback;

    public DeadLetterInspectionService(
            ConsumerFactory<String, Object> consumerFactory,
            String deadLetterTopic,
            String originalTopicFallback
    ) {
        this.consumerFactory = consumerFactory;
        this.deadLetterTopic = deadLetterTopic;
        this.originalTopicFallback = originalTopicFallback;
    }

    public Optional<DlqMessageView> peek(int partition, long offset) {
        TopicPartition tp = new TopicPartition(deadLetterTopic, partition);
        try (Consumer<String, Object> consumer = consumerFactory.createConsumer(
                "dlq-peek-" + UUID.randomUUID(), "peek-client")) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);
            for (ConsumerRecord<String, Object> record : consumer.poll(Duration.ofSeconds(10))) {
                if (record.offset() == offset) {
                    return Optional.of(toView(record));
                }
            }
            return Optional.empty();
        }
    }

    public List<DlqMessageView> peekFrom(int partition, long fromOffset, int max) {
        int limit = Math.min(Math.max(max, 1), 100);
        TopicPartition tp = new TopicPartition(deadLetterTopic, partition);
        List<DlqMessageView> out = new ArrayList<>();
        try (Consumer<String, Object> consumer = consumerFactory.createConsumer(
                "dlq-peek-" + UUID.randomUUID(), "peek-client")) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, fromOffset);
            for (ConsumerRecord<String, Object> record : consumer.poll(Duration.ofSeconds(10))) {
                out.add(toView(record));
                if (out.size() >= limit) {
                    break;
                }
            }
        }
        return out;
    }

    private DlqMessageView toView(ConsumerRecord<String, Object> record) {
        return new DlqMessageView(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value() == null ? null : record.value().getClass().getName(),
                DeadLetterHeaderEnricher.resolveOriginalTopic(record, originalTopicFallback),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.ORIGINAL_PARTITION).orElse(null),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.ORIGINAL_OFFSET).orElse(null),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.FAILURE_REASON).orElse(null),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.EXCEPTION_FQCN).orElse(null),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.EVENT_ID).orElse(null),
                DeadLetterHeaderEnricher.header(record.headers(), PlatformHeaders.REPROCESS_COUNT).orElse("0")
        );
    }

    public record DlqMessageView(
            String dlqTopic, int dlqPartition, long dlqOffset, String key, String valueType,
            String originalTopic, String originalPartition, String originalOffset,
            String failureReason, String exceptionFqcn, String eventId, String reprocessCount
    ) {
    }
}
