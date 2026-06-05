package com.ecommerce.orderservice.dto;

public record OrderRequest(
        String orderId,
        String productId,
        int quantity,
        String status
) {
}
