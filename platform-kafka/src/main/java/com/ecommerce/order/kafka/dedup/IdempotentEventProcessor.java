package com.ecommerce.order.kafka.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles "same message consumed multiple times" (at-least-once + rebalance + producer retry).
 *
 * <p>DLQ reprocess with {@code X-Force-Reprocess=true} calls {@link #clear(String)} so the
 * handler runs again after ops fixed the root cause.
 */
public class IdempotentEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventProcessor.class);
    private static final String REDIS_PREFIX = "ecommerce:idem:";

    private final ConcurrentHashMap<String, Boolean> localDedup = new ConcurrentHashMap<>();
    private final ObjectProvider<StringRedisTemplate> redis;

    public IdempotentEventProcessor(ObjectProvider<StringRedisTemplate> redis) {
        this.redis = redis;
    }

    /** Check only — does not mark. */
    public boolean alreadyProcessed(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            return false;
        }
        if (localDedup.containsKey(eventId)) {
            log.warn("DUPLICATE suppressed (local) eventId={}", eventId);
            return true;
        }
        StringRedisTemplate template = redis.getIfAvailable();
        if (template != null) {
            Boolean exists = template.hasKey(REDIS_PREFIX + eventId);
            if (Boolean.TRUE.equals(exists)) {
                log.warn("DUPLICATE suppressed (redis) eventId={}", eventId);
                return true;
            }
        }
        return false;
    }

    public void markProcessed(String eventId) {
        if (eventId == null) {
            return;
        }
        localDedup.put(eventId, Boolean.TRUE);
        StringRedisTemplate template = redis.getIfAvailable();
        if (template != null) {
            template.opsForValue().set(REDIS_PREFIX + eventId, "1", Duration.ofDays(7));
        }
    }

    /** Used by DLQ reprocess with force=true so the same eventId can run again. */
    public void clear(String eventId) {
        if (eventId == null) {
            return;
        }
        localDedup.remove(eventId);
        StringRedisTemplate template = redis.getIfAvailable();
        if (template != null) {
            template.delete(REDIS_PREFIX + eventId);
        }
        log.info("idempotency cleared for forced reprocess eventId={}", eventId);
    }

    public boolean processOnce(String eventId, Runnable work) {
        if (alreadyProcessed(eventId)) {
            return false;
        }
        work.run();
        markProcessed(eventId);
        return true;
    }
}
