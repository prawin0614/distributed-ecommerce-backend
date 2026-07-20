package com.ecommerce.orderservice.service;

import com.ecommerce.events.avro.InventoryReservationFailedEvent;
import com.ecommerce.events.avro.OrderCancelledEvent;
import com.ecommerce.events.avro.OrderConfirmedEvent;
import com.ecommerce.events.avro.PaymentCompletedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderSagaEventListener {
    private static final Logger logger = LoggerFactory.getLogger(OrderSagaEventListener.class);

    private final OrderService orderService;
    private final KafkaTemplate<String, OrderConfirmedEvent> orderConfirmedKafkaTemplate;
    private final KafkaTemplate<String, OrderCancelledEvent> orderCancelledKafkaTemplate;
    private final String orderConfirmedTopic;
    private final String orderCancelledTopic;

    public OrderSagaEventListener(
            OrderService orderService,
            KafkaTemplate<String, OrderConfirmedEvent> orderConfirmedKafkaTemplate,
            KafkaTemplate<String, OrderCancelledEvent> orderCancelledKafkaTemplate,
            @Value("${app.kafka.topics.order-confirmed}") String orderConfirmedTopic,
            @Value("${app.kafka.topics.order-cancelled}") String orderCancelledTopic) {
        this.orderService = orderService;
        this.orderConfirmedKafkaTemplate = orderConfirmedKafkaTemplate;
        this.orderCancelledKafkaTemplate = orderCancelledKafkaTemplate;
        this.orderConfirmedTopic = orderConfirmedTopic;
        this.orderCancelledTopic = orderCancelledTopic;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-completed}",
            groupId = "${app.kafka.consumer-groups.order}")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        try {
            String orderId = event.getOrderId().toString();
            logger.info("[SAGA] PaymentCompletedEvent received for orderId={}", orderId);
            orderService.confirmOrder(orderId);

            OrderConfirmedEvent confirmedEvent = OrderConfirmedEvent.newBuilder()
                    .setOrderId(orderId)
                    .setStatus("CONFIRMED")
                    .setMessage("Order confirmed and ready for fulfillment")
                    .build();

            orderConfirmedKafkaTemplate.send(orderConfirmedTopic, orderId, confirmedEvent);
            logger.info("[SAGA] Order confirmed for orderId={}", orderId);
        } catch (Exception ex) {
            logger.error("[SAGA] Failed handling PaymentCompletedEvent for orderId={}", event.getOrderId(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            groupId = "${app.kafka.consumer-groups.order}")
    public void onInventoryReservationFailed(InventoryReservationFailedEvent event) {
        try {
            String orderId = event.getOrderId().toString();
            String reason = event.getReason().toString();
            logger.info("[SAGA] InventoryReservationFailedEvent received for orderId={}", orderId);
            cancelAndPublish(orderId, reason);
        } catch (Exception ex) {
            logger.error("[SAGA] Failed handling InventoryReservationFailedEvent for orderId={}",
                    event.getOrderId(), ex);
            throw ex;
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-failed}",
            groupId = "${app.kafka.consumer-groups.order}")
    public void onPaymentFailed(PaymentFailedEvent event) {
        try {
            String orderId = event.getOrderId().toString();
            String reason = event.getReason().toString();
            logger.info("[SAGA] PaymentFailedEvent received for orderId={}", orderId);
            orderService.failOrder(orderId, reason);
            logger.info("[SAGA] Order marked failed for orderId={}", orderId);
        } catch (Exception ex) {
            logger.error("[SAGA] Failed handling PaymentFailedEvent for orderId={}", event.getOrderId(), ex);
            throw ex;
        }
    }

    private void cancelAndPublish(String orderId, String reason) {
        orderService.cancelOrder(orderId, reason);

        OrderCancelledEvent cancelledEvent = OrderCancelledEvent.newBuilder()
                .setOrderId(orderId)
                .setStatus("CANCELLED")
                .setReason(reason)
                .build();

        orderCancelledKafkaTemplate.send(orderCancelledTopic, orderId, cancelledEvent);
        logger.info("[SAGA] Order cancelled for orderId={}", orderId);
    }
}
