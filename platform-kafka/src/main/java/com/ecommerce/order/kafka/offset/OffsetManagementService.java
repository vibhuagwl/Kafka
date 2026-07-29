package com.ecommerce.order.kafka.offset;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Offset management algorithms for interviews — seek, replay, external store, recovery.
 *
 * <h2>Kafka internals</h2>
 * {@code commitSync} → OffsetCommitRequest to group coordinator (blocking).
 * {@code commitAsync} → same request, non-blocking + callback.
 * {@code seek} → local only until next fetch; does not commit.
 */
public class OffsetManagementService implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(OffsetManagementService.class);

    /** External offset table simulation (DB in later module). */
    private final ConcurrentHashMap<String, Long> externalOffsets = new ConcurrentHashMap<>();

    private final ThreadLocal<ConsumerSeekCallback> seekCallback = new ThreadLocal<>();

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        seekCallback.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        seekCallback.set(callback);
        assignments.forEach((tp, offset) -> {
            Long external = externalOffsets.get(key(tp));
            if (external != null) {
                log.info("EXTERNAL offset recovery {} → {}", tp, external);
                callback.seek(tp.topic(), tp.partition(), external);
            }
        });
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // no-op
    }

    public void storeExternal(TopicPartition tp, long nextOffset) {
        externalOffsets.put(key(tp), nextOffset);
    }

    public Long getExternal(TopicPartition tp) {
        return externalOffsets.get(key(tp));
    }

    /** Replay from offset (inclusive) — seek then continue polling. */
    public void replayFromOffset(String topic, int partition, long offset) {
        ConsumerSeekCallback cb = seekCallback.get();
        if (cb == null) {
            throw new IllegalStateException("No seek callback — call from listener thread after assign");
        }
        log.warn("OFFSET REPLAY topic={} partition={} offset={}", topic, partition, offset);
        cb.seek(topic, partition, offset);
    }

    public void replayFromTimestamp(String topic, int partition, long timestampEpochMs) {
        ConsumerSeekCallback cb = seekCallback.get();
        if (cb == null) {
            throw new IllegalStateException("No seek callback registered");
        }
        cb.seekToTimestamp(topic, partition, timestampEpochMs);
    }

    public void seekToBeginning(String topic, int partition) {
        ConsumerSeekCallback cb = requireCallback();
        cb.seekToBeginning(topic, partition);
    }

    public void seekToEnd(String topic, int partition) {
        ConsumerSeekCallback cb = requireCallback();
        cb.seekToEnd(topic, partition);
    }

    /**
     * Manual sync commit — interview: blocking, stronger delivery guarantee for at-least-once
     * when paired with process-then-commit.
     */
    public void commitSync(Consumer<?, ?> consumer, TopicPartition tp, long offset) {
        Map<TopicPartition, OffsetAndMetadata> map = Map.of(tp, new OffsetAndMetadata(offset));
        consumer.commitSync(map);
        log.debug("commitSync {} → {}", tp, offset);
    }

    public void commitAsync(Consumer<?, ?> consumer, TopicPartition tp, long offset) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(tp, new OffsetAndMetadata(offset));
        consumer.commitAsync(map, (offsets, exception) -> {
            if (exception != null) {
                log.error("commitAsync failed {}", offsets, exception);
            }
        });
    }

    /** Offset corruption handling — reset to earliest for given partitions. */
    public void recoverCorruptedOffsets(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.error("OFFSET CORRUPTION recovery — seekToBeginning {}", partitions);
        consumer.seekToBeginning(partitions);
        consumer.commitSync();
    }

    private ConsumerSeekCallback requireCallback() {
        ConsumerSeekCallback cb = seekCallback.get();
        if (cb == null) {
            throw new IllegalStateException("Seek callback not available on this thread");
        }
        return cb;
    }

    private static String key(TopicPartition tp) {
        return tp.topic() + "-" + tp.partition();
    }
}
