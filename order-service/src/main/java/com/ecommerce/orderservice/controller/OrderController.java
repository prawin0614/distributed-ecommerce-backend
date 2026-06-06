package com.ecommerce.orderservice.controller;

import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class OrderController {
    private static final Logger logger = LoggerFactory.getLogger(OrderController.class);
    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/orders")
    public ResponseEntity<?> createOrder(@RequestBody OrderRequest request) {
        logger.debug("Received create order request: {}", request);
        try {
            Order savedOrder = orderService.createOrder(request);
            logger.info("Order created successfully: {}", savedOrder.getOrderId());
            return ResponseEntity.status(HttpStatus.CREATED).body(savedOrder);
        } catch (IllegalArgumentException ex) {
            logger.warn("Invalid create order request: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Error creating order", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("ORDER_CREATION_FAILED", "Failed to create order: " + ex.getMessage()));
        }
    }

    @GetMapping("/orders")
    public ResponseEntity<?> getAllOrders() {
        logger.debug("Received get all orders request");
        try {
            List<Order> orders = orderService.getAllOrders();
            logger.info("Retrieved {} orders", orders.size());
            return ResponseEntity.ok(orders);
        } catch (Exception ex) {
            logger.error("Error retrieving orders", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("ORDER_RETRIEVAL_FAILED", "Failed to retrieve orders: " + ex.getMessage()));
        }
    }

    static class ErrorResponse {
        public String code;
        public String message;

        public ErrorResponse(String code, String message) {
            this.code = code;
            this.message = message;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }
}
