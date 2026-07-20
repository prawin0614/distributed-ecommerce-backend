package com.ecommerce.paymentservice.service;

import com.ecommerce.events.avro.InventoryReservedEvent;
import com.ecommerce.events.avro.OrderCancelledEvent;
import com.ecommerce.events.avro.PaymentCompletedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventListener {
    private static final Logger logger = LoggerFactory.getLogger(PaymentEventListener.class);
    private final PaymentService paymentService;
    private final KafkaTemplate<String, PaymentCompletedEvent> paymentCompletedKafkaTemplate;
    private final KafkaTemplate<String, PaymentFailedEvent> paymentFailedKafkaTemplate;
    private final String paymentCompletedTopic;
    private final String paymentFailedTopic;

    public PaymentEventListener(
            PaymentService paymentService,
            KafkaTemplate<String, PaymentCompletedEvent> paymentCompletedKafkaTemplate,
            KafkaTemplate<String, PaymentFailedEvent> paymentFailedKafkaTemplate,
            @Value("${app.kafka.topics.payment-completed}") String paymentCompletedTopic,
            @Value("${app.kafka.topics.payment-failed}") String paymentFailedTopic) {
        this.paymentService = paymentService;
        this.paymentCompletedKafkaTemplate = paymentCompletedKafkaTemplate;
        this.paymentFailedKafkaTemplate = paymentFailedKafkaTemplate;
        this.paymentCompletedTopic = paymentCompletedTopic;
        this.paymentFailedTopic = paymentFailedTopic;
    }

    @PostConstruct
    public void logStartup() {
        logger.info("Consumer started for topic inventory-reserved");
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reserved}",
            groupId = "${app.kafka.consumer-groups.payment}")
    public void handleOrderCreated(InventoryReservedEvent event) {
        logger.info("[SAGA] InventoryReservedEvent received for orderId={}", event.getOrderId());

        String orderId = event.getOrderId().toString();
        String productId = event.getProductId().toString();
        int reservedQuantity = event.getReservedQuantity();

        PaymentService.PaymentProcessingResult result =
            paymentService.processPaymentFromInventory(orderId, reservedQuantity);

        if (result.success()) {
            PaymentCompletedEvent completedEvent = PaymentCompletedEvent.newBuilder()
                .setOrderId(result.orderId())
                .setPaymentId(result.paymentId())
                .setStatus("COMPLETED")
                .build();
            paymentCompletedKafkaTemplate.send(paymentCompletedTopic, result.orderId(), completedEvent);
            logger.info("[SAGA] Payment completed for orderId={}",
                result.orderId());
            return;
        }

        PaymentFailedEvent failedEvent = PaymentFailedEvent.newBuilder()
            .setOrderId(result.orderId())
            .setProductId(productId)
            .setReservedQuantity(reservedQuantity)
            .setReason(result.reason())
            .setStatus("FAILED")
            .build();
        paymentFailedKafkaTemplate.send(paymentFailedTopic, result.orderId(), failedEvent);
        logger.info("[SAGA] Payment failed for orderId={}",
            result.orderId());
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-cancelled}",
            groupId = "${app.kafka.consumer-groups.payment}")
    public void handleOrderCancelled(OrderCancelledEvent event) {
        String orderId = event.getOrderId().toString();
        logger.info("[SAGA] OrderCancelledEvent received for orderId={}", orderId);
        paymentService.refundPayment(orderId);
    }
}
