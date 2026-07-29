package com.ecommerce.order.inventoryservice.inbox;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboxService {

    private final InboxEventRepository repository;

    public InboxService(InboxEventRepository repository) {
        this.repository = repository;
    }

    /**
     * @return true if this is the first time seeing eventId (should process)
     */
    @Transactional
    public boolean tryAccept(String eventId, String consumerName) {
        if (eventId == null || repository.existsById(eventId)) {
            return false;
        }
        InboxEventEntity row = new InboxEventEntity();
        row.setEventId(eventId);
        row.setConsumerName(consumerName);
        try {
            repository.saveAndFlush(row);
            return true;
        } catch (DataIntegrityViolationException ex) {
            return false;
        }
    }

    @Transactional
    public void clear(String eventId) {
        if (eventId != null) {
            repository.deleteById(eventId);
        }
    }
}
