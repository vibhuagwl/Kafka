package com.ecommerce.order.common.event;

/**
 * Canonical event type names used in headers and envelopes.
 */
public final class EventTypes {

    public static final String ORDER_PLACED = "OrderPlaced";
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_REJECTED = "InventoryRejected";
    public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String SHIPMENT_CREATED = "ShipmentCreated";
    public static final String NOTIFICATION_REQUESTED = "NotificationRequested";
    public static final String AUDIT_RECORDED = "AuditRecorded";

    private EventTypes() {
    }
}
