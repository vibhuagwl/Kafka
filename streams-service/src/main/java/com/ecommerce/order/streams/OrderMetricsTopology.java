package com.ecommerce.order.streams;

import com.ecommerce.order.common.messaging.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka Streams topology (Q79–Q83):
 * <ul>
 *   <li>Stateful count per customer (RocksDB state store)</li>
 *   <li>Tumbling window of 1 minute</li>
 *   <li>Stream-table style enrichment via KTable of order counts</li>
 * </ul>
 */
@Component
public class OrderMetricsTopology {

    public static final String CUSTOMER_ORDER_COUNTS = "customer-order-counts";
    public static final String CUSTOMER_ORDER_WINDOW = "customer-order-windowed";

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        KStream<String, String> orders = builder.stream(
                KafkaTopics.ORDERS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Q80/Q81 — stateful aggregation backed by RocksDB
        KTable<String, Long> counts = orders
                .groupBy((key, value) -> extractCustomer(key, value), Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore("order-count-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        counts.toStream().to(CUSTOMER_ORDER_COUNTS, Produced.with(Serdes.String(), Serdes.Long()));

        // Q82 — tumbling windows
        orders
                .groupBy((key, value) -> extractCustomer(key, value), Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("order-window-store"))
                .toStream()
                .map((windowedKey, count) -> org.apache.kafka.streams.KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start(),
                        count
                ))
                .to(CUSTOMER_ORDER_WINDOW, Produced.with(Serdes.String(), Serdes.Long()));

        // Q83 — stream-table join (orders joined with running counts)
        orders.join(
                counts,
                (orderValue, count) -> "order=" + orderValue + ";customerOrders=" + count
        ).to("orders-enriched", Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String extractCustomer(String key, String value) {
        if (value != null && value.contains("customerId")) {
            int i = value.indexOf("customerId");
            return value.substring(Math.max(0, i), Math.min(value.length(), i + 40));
        }
        return key == null ? "unknown" : key;
    }
}
