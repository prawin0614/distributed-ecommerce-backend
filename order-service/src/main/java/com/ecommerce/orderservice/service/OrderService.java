package com.ecommerce.orderservice.service;

import com.ecommerce.events.avro.OrderCreatedEvent;
import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.repository.OrderRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import java.time.Duration;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

@Service
public class OrderService {
    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String orderCreatedTopic;

    public OrderService(
            OrderRepository orderRepository,
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
    }

    public Order createOrder(OrderRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("OrderRequest must not be null");
        }
        if (request.orderId() == null || request.orderId().isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (request.productId() == null || request.productId().isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (request.quantity() <= 0) {
            throw new IllegalArgumentException("quantity must be greater than 0");
        }

        Order order = new Order();
        order.setOrderId(request.orderId());
        order.setProductId(request.productId());
        order.setQuantity(request.quantity());
        order.setStatus("PENDING");

        logger.debug("Saving order entity: {}", order);
        Order savedOrder = orderRepository.save(order);
        logger.debug("Saved order entity: {}", savedOrder);

        OrderCreatedEvent event = OrderCreatedEvent.newBuilder()
            .setOrderId(savedOrder.getOrderId())
            .setProductId(savedOrder.getProductId())
            .setQuantity(savedOrder.getQuantity())
            .setStatus(savedOrder.getStatus())
            .build();

        try {
            kafkaTemplate.send(orderCreatedTopic, savedOrder.getOrderId(), event)
                .get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
            logger.info("[SAGA] OrderCreatedEvent published for orderId={} to topic={}",
                savedOrder.getOrderId(), orderCreatedTopic);
        } catch (Exception ex) {
            logger.error("Failed to publish OrderCreatedEvent for orderId={}", savedOrder.getOrderId(), ex);
            throw new RuntimeException("Order saved but event publication failed", ex);
        }

        return savedOrder;
    }

    @Transactional
    @Retry(name = "orderStateUpdate", fallbackMethod = "confirmOrderFallback")
    @CircuitBreaker(name = "orderStateUpdate", fallbackMethod = "confirmOrderFallback")
    @RateLimiter(name = "orderStateUpdate", fallbackMethod = "confirmOrderFallback")
    public Order confirmOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("Order not found for orderId: " + orderId));
        order.setStatus("CONFIRMED");
        return orderRepository.save(order);
    }

    @Transactional
    @Retry(name = "orderStateUpdate", fallbackMethod = "cancelOrderFallback")
    @CircuitBreaker(name = "orderStateUpdate", fallbackMethod = "cancelOrderFallback")
    @RateLimiter(name = "orderStateUpdate", fallbackMethod = "cancelOrderFallback")
    public Order cancelOrder(String orderId, String reason) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("Order not found for orderId: " + orderId));
        order.setStatus("CANCELLED");
        logger.info("[SAGA] Order {} cancelled. reason={}", orderId, reason);
        return orderRepository.save(order);
    }

    @Transactional
    @Retry(name = "orderStateUpdate", fallbackMethod = "failOrderFallback")
    @CircuitBreaker(name = "orderStateUpdate", fallbackMethod = "failOrderFallback")
    @RateLimiter(name = "orderStateUpdate", fallbackMethod = "failOrderFallback")
    public Order failOrder(String orderId, String reason) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("Order not found for orderId: " + orderId));
        order.setStatus("FAILED");
        logger.info("[SAGA] Order {} failed. reason={}", orderId, reason);
        return orderRepository.save(order);
    }

    private Order confirmOrderFallback(String orderId, Throwable ex) {
        logger.error("[SAGA] confirmOrder fallback for orderId={} reason={}",
                orderId, ex.getMessage(), ex);
        throw new IllegalStateException("Unable to confirm order due to resilience fallback: " + orderId, ex);
    }

    private Order cancelOrderFallback(String orderId, String reason, Throwable ex) {
        logger.error("[SAGA] cancelOrder fallback for orderId={} reason={}",
                orderId, ex.getMessage(), ex);
        throw new IllegalStateException("Unable to cancel order due to resilience fallback: " + orderId, ex);
    }

    private Order failOrderFallback(String orderId, String reason, Throwable ex) {
        logger.error("[SAGA] failOrder fallback for orderId={} reason={}",
                orderId, ex.getMessage(), ex);
        throw new IllegalStateException("Unable to fail order due to resilience fallback: " + orderId, ex);
    }

    public List<Order> getAllOrders() {
        logger.debug("Fetching all orders");
        try {
            List<Order> orders = orderRepository.findAll();
            logger.debug("Retrieved {} orders", orders.size());
            return orders;
        } catch (Exception ex) {
            logger.error("Error retrieving orders", ex);
            throw new RuntimeException("Failed to retrieve orders: " + ex.getMessage(), ex);
        }
    }
}
