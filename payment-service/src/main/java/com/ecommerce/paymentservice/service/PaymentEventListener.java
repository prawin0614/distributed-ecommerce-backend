package com.ecommerce.paymentservice.service;

import com.ecommerce.paymentservice.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// WEEK 2 FEATURE - TEMPORARILY DISABLED
// Kafka listener wiring will be restored in Week 2.
public class PaymentEventListener {
    private static final Logger logger = LoggerFactory.getLogger(PaymentEventListener.class);

    @PostConstruct
    public void logStartup() {
        logger.info("Consumer started for topic order-created");
    }

    @KafkaListener(topics = "order-created", groupId = "payment-group")
    public void handleOrderCreated(OrderCreatedEvent event) {
        logger.info("Consumer received event: {}", event);
        logger.info("Payment processed for orderId: {}", event.getOrderId());
    }
}
