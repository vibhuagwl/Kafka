package com.ecommerce.order.kafka.dlq;

import com.ecommerce.order.kafka.dedup.IdempotentEventProcessor;
import com.ecommerce.order.kafka.messaging.PlatformHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DeadLetterReprocessService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReprocessService.class);

    private final ConsumerFactory<String, Object> consumerFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String deadLetterTopic;
    private final String originalTopicFallback;
    private final ObjectProvider<IdempotentEventProcessor> idempotent;

    public DeadLetterReprocessService(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            String deadLetterTopic,
            String originalTopicFallback,
            ObjectProvider<IdempotentEventProcessor> idempotent
    ) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterTopic = deadLetterTopic;
        this.originalTopicFallback = originalTopicFallback;
        this.idempotent = idempotent;
    }

    public ReprocessResult reprocessByOffset(int dlqPartition, long dlqOffset, boolean force) {
        TopicPartition tp = new TopicPartition(deadLetterTopic, dlqPartition);
        try (Consumer<String, Object> consumer = consumerFactory.createConsumer(
                "dlq-reprocess-" + UUID.randomUUID(), "reprocess-client")) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, dlqOffset);
            ConsumerRecords<String, Object> polled = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, Object> record : polled) {
                if (record.partition() == dlqPartition && record.offset() == dlqOffset) {
                    return republish(record, force);
                }
            }
            throw new DlqReprocessException("No DLQ record at " + tp + " offset=" + dlqOffset);
        }
    }

    public List<ReprocessResult> reprocessRange(int dlqPartition, long fromOffset, long toOffset, boolean force) {
        if (toOffset < fromOffset) {
            throw new IllegalArgumentException("toOffset < fromOffset");
        }
        if (toOffset - fromOffset > 500) {
            throw new IllegalArgumentException("Range too large (max 500)");
        }
        List<ReprocessResult> results = new ArrayList<>();
        for (long offset = fromOffset; offset <= toOffset; offset++) {
            try {
                results.add(reprocessByOffset(dlqPartition, offset, force));
            } catch (DlqReprocessException ex) {
                results.add(ReprocessResult.failed(dlqPartition, offset, ex.getMessage()));
            }
        }
        return results;
    }

    public ReprocessResult republish(ConsumerRecord<String, Object> dlqRecord, boolean force) {
        if (force) {
            IdempotentEventProcessor processor = idempotent.getIfAvailable();
            if (processor != null) {
                DeadLetterHeaderEnricher.header(dlqRecord.headers(), PlatformHeaders.EVENT_ID)
                        .ifPresent(processor::clear);
                DeadLetterHeaderEnricher.header(dlqRecord.headers(), PlatformHeaders.IDEMPOTENCY_KEY)
                        .ifPresent(processor::clear);
            }
        }

        String originalTopic = DeadLetterHeaderEnricher.resolveOriginalTopic(dlqRecord, originalTopicFallback);
        String reprocessId = UUID.randomUUID().toString();
        int priorCount = DeadLetterHeaderEnricher.header(dlqRecord.headers(), PlatformHeaders.REPROCESS_COUNT)
                .map(v -> {
                    try {
                        return Integer.parseInt(v);
                    } catch (NumberFormatException e) {
                        return 0;
                    }
                })
                .orElse(0);

        Headers headers = DeadLetterHeaderEnricher.copyHeaders(dlqRecord.headers());
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.REPROCESSED_FROM_DLQ, "true");
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.REPROCESS_ID, reprocessId);
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.REPROCESS_COUNT, String.valueOf(priorCount + 1));
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.FORCE_REPROCESS, String.valueOf(force));
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.DLQ_TOPIC, dlqRecord.topic());
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.DLQ_PARTITION, String.valueOf(dlqRecord.partition()));
        DeadLetterHeaderEnricher.put(headers, PlatformHeaders.DLQ_OFFSET, String.valueOf(dlqRecord.offset()));

        ProducerRecord<String, Object> out = new ProducerRecord<>(
                originalTopic, null, System.currentTimeMillis(),
                dlqRecord.key(), dlqRecord.value(), headers
        );

        try {
            SendResult<String, Object> sent = kafkaTemplate.send(out).get();
            log.info("DLQ reprocess ok {}@{} → {}-{}-{}",
                    dlqRecord.partition(), dlqRecord.offset(),
                    sent.getRecordMetadata().topic(),
                    sent.getRecordMetadata().partition(),
                    sent.getRecordMetadata().offset());
            return ReprocessResult.ok(
                    dlqRecord.partition(), dlqRecord.offset(), originalTopic,
                    sent.getRecordMetadata().partition(), sent.getRecordMetadata().offset(),
                    reprocessId, force);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DlqReprocessException("Interrupted while republishing from DLQ", e);
        } catch (ExecutionException e) {
            throw new DlqReprocessException("Failed to republish from DLQ to " + originalTopic, e.getCause());
        }
    }

    public record ReprocessResult(
            boolean success, int dlqPartition, long dlqOffset, String originalTopic,
            Integer republishedPartition, Long republishedOffset, String reprocessId,
            boolean force, String error
    ) {
        static ReprocessResult ok(int p, long o, String topic, int rp, long ro, String id, boolean force) {
            return new ReprocessResult(true, p, o, topic, rp, ro, id, force, null);
        }

        static ReprocessResult failed(int p, long o, String error) {
            return new ReprocessResult(false, p, o, null, null, null, null, false, error);
        }
    }
}
