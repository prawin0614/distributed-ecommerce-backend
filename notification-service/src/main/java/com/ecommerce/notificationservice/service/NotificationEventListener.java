package com.ecommerce.notificationservice.service;

import com.ecommerce.events.avro.InventoryReleasedEvent;
import com.ecommerce.events.avro.OrderCancelledEvent;
import com.ecommerce.events.avro.OrderConfirmedEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationEventListener {
    private static final Logger logger = LoggerFactory.getLogger(NotificationEventListener.class);

    @KafkaListener(
            topics = "${app.kafka.topics.order-confirmed}",
            groupId = "${app.kafka.consumer-groups.notification}")
        @Retry(name = "notificationOps", fallbackMethod = "onOrderConfirmedFallback")
        @CircuitBreaker(name = "notificationOps", fallbackMethod = "onOrderConfirmedFallback")
        @RateLimiter(name = "notificationOps", fallbackMethod = "onOrderConfirmedFallback")
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        logger.info("[SAGA] Order confirmed notification published for orderId={}", event.getOrderId());
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-cancelled}",
            groupId = "${app.kafka.consumer-groups.notification}")
    @Retry(name = "notificationOps", fallbackMethod = "onOrderCancelledFallback")
    @CircuitBreaker(name = "notificationOps", fallbackMethod = "onOrderCancelledFallback")
    @RateLimiter(name = "notificationOps", fallbackMethod = "onOrderCancelledFallback")
    public void onOrderCancelled(OrderCancelledEvent event) {
        logger.info("[SAGA] Order failure notification published for orderId={} reason={}",
                event.getOrderId(), event.getReason());
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released}",
            groupId = "${app.kafka.consumer-groups.notification}")
        @Retry(name = "notificationOps", fallbackMethod = "onInventoryReleasedFallback")
        @CircuitBreaker(name = "notificationOps", fallbackMethod = "onInventoryReleasedFallback")
        @RateLimiter(name = "notificationOps", fallbackMethod = "onInventoryReleasedFallback")
    public void onInventoryReleased(InventoryReleasedEvent event) {
        logger.info("[SAGA] Inventory released notification published for orderId={} productId={}",
                event.getOrderId(), event.getProductId());
    }

        public void onOrderConfirmedFallback(OrderConfirmedEvent event, Throwable ex) {
                logger.error("[SAGA] Notification fallback order-confirmed orderId={} reason={}",
                                event.getOrderId(), ex.getMessage(), ex);
        }

        public void onOrderCancelledFallback(OrderCancelledEvent event, Throwable ex) {
                logger.error("[SAGA] Notification fallback order-cancelled orderId={} reason={}",
                                event.getOrderId(), ex.getMessage(), ex);
        }

        public void onInventoryReleasedFallback(InventoryReleasedEvent event, Throwable ex) {
                logger.error("[SAGA] Notification fallback inventory-released orderId={} reason={}",
                                event.getOrderId(), ex.getMessage(), ex);
        }
}
