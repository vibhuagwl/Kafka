package com.ecommerce.order.kafka.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom partition assignor — interview favorite.
 *
 * <p>Extends {@link AbstractPartitionAssignor} (same base as Range/Sticky internals).
 * Group leader computes assignment during SyncGroup; members apply locally.
 *
 * <h2>Advantages</h2>
 * Deterministic affinity: partition → member via stable hash; fewer surprises than RR.
 *
 * <h2>Limitations</h2>
 * Eager only here; must be identical on all members; prefer CooperativeSticky unless
 * you have a hard tenancy/affinity requirement.
 */
public class OrderAffinityAssignor extends AbstractPartitionAssignor {

    public static final String NAME = "order-affinity";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions
    ) {
        List<String> members = new ArrayList<>(subscriptions.keySet());
        members.sort(Comparator.naturalOrder());

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        members.forEach(m -> assignment.put(m, new ArrayList<>()));

        List<TopicPartition> all = new ArrayList<>();
        for (Map.Entry<String, Integer> e : partitionsPerTopic.entrySet()) {
            for (int p = 0; p < e.getValue(); p++) {
                all.add(new TopicPartition(e.getKey(), p));
            }
        }
        all.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

        if (members.isEmpty()) {
            return assignment;
        }

        for (TopicPartition tp : all) {
            // Only assign if at least one member subscribed to the topic
            boolean subscribed = subscriptions.values().stream().anyMatch(s -> s.topics().contains(tp.topic()));
            if (!subscribed) {
                continue;
            }
            List<String> eligible = members.stream()
                    .filter(m -> subscriptions.get(m).topics().contains(tp.topic()))
                    .toList();
            if (eligible.isEmpty()) {
                continue;
            }
            int idx = Math.floorMod(tp.topic().hashCode() * 31 + tp.partition(), eligible.size());
            assignment.get(eligible.get(idx)).add(tp);
        }
        return assignment;
    }
}
