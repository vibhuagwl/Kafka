package com.ecommerce.order.orderservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    @Query(value = """
            SELECT * FROM outbox_event
            WHERE status = 'NEW'
            ORDER BY id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockBatchForPublish();
}
