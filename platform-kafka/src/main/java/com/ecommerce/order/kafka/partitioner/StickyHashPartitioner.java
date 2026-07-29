package com.ecommerce.order.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic producer partitioner: keyed hash, sticky, or round-robin for null keys.
 * Configure mode via {@code platform.kafka.partitioner-mode} /
 * producer config {@code platform.partitioner.mode}.
 */
public class StickyHashPartitioner implements Partitioner {

    public static final String NAME = StickyHashPartitioner.class.getName();
    private static final Logger log = LoggerFactory.getLogger(StickyHashPartitioner.class);

    public enum Mode { HASH_OR_STICKY, HASH_OR_ROUND_ROBIN }

    private Mode mode = Mode.HASH_OR_STICKY;
    private final ConcurrentHashMap<String, StickyState> sticky = new ConcurrentHashMap<>();
    private final AtomicInteger rr = new AtomicInteger();

    @Override
    public void configure(Map<String, ?> configs) {
        Object m = configs.get("platform.partitioner.mode");
        if (m == null) {
            m = configs.get("ecommerce.partitioner.mode");
        }
        if (m != null) {
            mode = Mode.valueOf(m.toString());
        }
        log.info("StickyHashPartitioner mode={}", mode);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int num = partitions.size();
        if (num == 0) {
            return 0;
        }
        if (keyBytes != null && keyBytes.length > 0) {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % num;
        }
        if (mode == Mode.HASH_OR_ROUND_ROBIN) {
            return Utils.toPositive(rr.getAndIncrement()) % num;
        }
        return sticky.computeIfAbsent(topic, t -> new StickyState(num)).next(num);
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        StickyState state = sticky.get(topic);
        if (state != null) {
            state.rotate(cluster.partitionCountForTopic(topic));
        }
    }

    @Override
    public void close() {
        sticky.clear();
    }

    private static final class StickyState {
        private final AtomicInteger index;
        private final AtomicInteger recordsInBatch = new AtomicInteger();

        StickyState(int numPartitions) {
            this.index = new AtomicInteger(Math.floorMod(System.identityHashCode(this), Math.max(numPartitions, 1)));
        }

        int next(int numPartitions) {
            if (recordsInBatch.incrementAndGet() > 32) {
                rotate(numPartitions);
            }
            return Math.floorMod(index.get(), numPartitions);
        }

        void rotate(Integer numPartitions) {
            int n = numPartitions == null || numPartitions == 0 ? 1 : numPartitions;
            index.incrementAndGet();
            recordsInBatch.set(0);
            index.set(Math.floorMod(index.get(), n));
        }
    }
}
