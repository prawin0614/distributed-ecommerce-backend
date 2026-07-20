package com.ecommerce.inventoryservice.service;

import com.ecommerce.events.avro.InventoryReleasedEvent;
import com.ecommerce.events.avro.InventoryReservationFailedEvent;
import com.ecommerce.events.avro.InventoryReservedEvent;
import com.ecommerce.events.avro.OrderCreatedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InventoryEventListener {
    private static final Logger logger = LoggerFactory.getLogger(InventoryEventListener.class);
    private final InventoryService inventoryService;
    private final KafkaTemplate<String, InventoryReservedEvent> inventoryReservedKafkaTemplate;
    private final KafkaTemplate<String, InventoryReservationFailedEvent> inventoryReservationFailedKafkaTemplate;
    private final KafkaTemplate<String, InventoryReleasedEvent> inventoryReleasedKafkaTemplate;
    private final String inventoryReservedTopic;
    private final String inventoryReservationFailedTopic;
    private final String inventoryReleasedTopic;

    public InventoryEventListener(
            InventoryService inventoryService,
            KafkaTemplate<String, InventoryReservedEvent> inventoryReservedKafkaTemplate,
            KafkaTemplate<String, InventoryReservationFailedEvent> inventoryReservationFailedKafkaTemplate,
            KafkaTemplate<String, InventoryReleasedEvent> inventoryReleasedKafkaTemplate,
            @Value("${app.kafka.topics.inventory-reserved}") String inventoryReservedTopic,
            @Value("${app.kafka.topics.inventory-reservation-failed}") String inventoryReservationFailedTopic,
            @Value("${app.kafka.topics.inventory-released}") String inventoryReleasedTopic) {
        this.inventoryService = inventoryService;
        this.inventoryReservedKafkaTemplate = inventoryReservedKafkaTemplate;
        this.inventoryReservationFailedKafkaTemplate = inventoryReservationFailedKafkaTemplate;
        this.inventoryReleasedKafkaTemplate = inventoryReleasedKafkaTemplate;
        this.inventoryReservedTopic = inventoryReservedTopic;
        this.inventoryReservationFailedTopic = inventoryReservationFailedTopic;
        this.inventoryReleasedTopic = inventoryReleasedTopic;
    }

    @PostConstruct
    public void logStartup() {
        logger.info("Consumer started for topic order-created");
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${app.kafka.consumer-groups.inventory}")
    public void handleOrderCreated(OrderCreatedEvent event) {
        String orderId = event.getOrderId().toString();
        String productId = event.getProductId().toString();
        int quantity = event.getQuantity();
        logger.info("[SAGA] OrderCreatedEvent received for orderId={}", orderId);

        try {
            inventoryService.reserveInventory(productId, quantity);
            InventoryReservedEvent reservedEvent = InventoryReservedEvent.newBuilder()
                .setOrderId(orderId)
                .setProductId(productId)
                .setReservedQuantity(quantity)
                .setStatus("RESERVED")
                .build();

            inventoryReservedKafkaTemplate.send(inventoryReservedTopic, orderId, reservedEvent);
            logger.info("[SAGA] Inventory reserved for orderId={}", orderId);
        } catch (Exception ex) {
            InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.newBuilder()
                .setOrderId(orderId)
                .setProductId(productId)
                .setRequestedQuantity(quantity)
                .setReason(ex.getMessage())
                .setStatus("FAILED")
                .build();
            inventoryReservationFailedKafkaTemplate.send(inventoryReservationFailedTopic, orderId, failedEvent);
            logger.info("[SAGA] Inventory reservation failed for orderId={}", orderId);
        }
        }

        @KafkaListener(
            topics = "${app.kafka.topics.payment-failed}",
            groupId = "${app.kafka.consumer-groups.inventory}")
        public void handlePaymentFailed(PaymentFailedEvent event) {
        String orderId = event.getOrderId().toString();
        String productId = event.getProductId().toString();
        int reservedQuantity = event.getReservedQuantity();

        logger.info("[SAGA] Payment failed for orderId={}, triggering inventory compensation", orderId);
        inventoryService.releaseReservedInventory(productId, reservedQuantity);

        InventoryReleasedEvent releasedEvent = InventoryReleasedEvent.newBuilder()
            .setOrderId(orderId)
            .setProductId(productId)
            .setReleasedQuantity(reservedQuantity)
            .setStatus("RELEASED")
            .setReason("PAYMENT_FAILED_COMPENSATION")
            .build();

        inventoryReleasedKafkaTemplate.send(inventoryReleasedTopic, orderId, releasedEvent);
        logger.info("[SAGA] Inventory released for orderId={}", orderId);
    }
}
