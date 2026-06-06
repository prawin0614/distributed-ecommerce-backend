package com.ecommerce.inventoryservice.service;

import com.ecommerce.inventoryservice.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// WEEK 2 FEATURE - TEMPORARILY DISABLED
// This Kafka consumer will be enabled in Week 2
// @Service
public class InventoryEventListener {
    private static final Logger logger = LoggerFactory.getLogger(InventoryEventListener.class);

    @PostConstruct
    public void logStartup() {
        logger.info("Consumer started for topic order-created");
    }

    // @KafkaListener(topics = "order-created", groupId = "inventory-group")
    public void handleOrderCreated(OrderCreatedEvent event) {
        logger.info("Consumer received event: {}", event);
        logger.info("Inventory updated for productId: {}", event.getProductId());
    }
}
