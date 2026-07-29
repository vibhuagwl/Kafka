package com.ecommerce.order.inventoryservice.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxEventRepository extends JpaRepository<InboxEventEntity, String> {
}
