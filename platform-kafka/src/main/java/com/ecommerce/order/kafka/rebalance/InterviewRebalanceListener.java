package com.ecommerce.order.kafka.rebalance;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ConsumerAwareRebalanceListener} — maps to Kafka rebalance callbacks.
 *
 * <p>Internals: group coordinator detects member join/leave/timeout →
 * JoinGroup → SyncGroup → revoke/assign. CooperativeSticky does incremental revoke.
 *
 * <p>Interview: commit offsets on revoke (eager) to avoid duplicates; with
 * cooperative, only revoked partitions are paused.
 */
public class InterviewRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(InterviewRebalanceListener.class);

    private final AtomicInteger rebalanceCount = new AtomicInteger();

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.warn("REBALANCE revoked-before-commit count={} partitions={}",
                rebalanceCount.incrementAndGet(), partitions);
        // Last-chance sync commit to reduce duplicate processing after rebalance
        try {
            consumer.commitSync();
        } catch (Exception ex) {
            log.error("commitSync during revoke failed", ex);
        }
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("REBALANCE revoked-after-commit partitions={}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("REBALANCE assigned partitions={} position snapshot starting", partitions);
        for (TopicPartition tp : partitions) {
            try {
                long pos = consumer.position(tp);
                log.info("ASSIGNED {} position={}", tp, pos);
            } catch (Exception ex) {
                log.warn("Could not read position for {}", tp, ex);
            }
        }
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.error("REBALANCE partitions LOST (fatal for fencing) {}", partitions);
    }

    public int rebalanceCount() {
        return rebalanceCount.get();
    }
}
