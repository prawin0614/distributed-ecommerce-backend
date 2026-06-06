package com.ecommerce.orderservice.service;

import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

@Service
public class OrderService {
    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);
    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
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

        String status = request.status();
        if (status == null || status.isBlank()) {
            status = "CREATED";
        }

        Order order = new Order();
        order.setOrderId(request.orderId());
        order.setProductId(request.productId());
        order.setQuantity(request.quantity());
        order.setStatus(status);

        logger.debug("Saving order entity: {}", order);
        Order savedOrder = orderRepository.save(order);
        logger.debug("Saved order entity: {}", savedOrder);

        // WEEK 2 FEATURE - TEMPORARILY DISABLED
        // Kafka event publication will be restored in Week 2.

        return savedOrder;
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
