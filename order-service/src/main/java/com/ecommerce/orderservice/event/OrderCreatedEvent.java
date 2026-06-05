package com.ecommerce.orderservice.event;

public record OrderCreatedEvent(
        String orderId,
        String productId,
        int quantity,
        String status
) {
}
