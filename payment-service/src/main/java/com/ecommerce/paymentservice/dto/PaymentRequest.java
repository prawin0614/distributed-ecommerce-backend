package com.ecommerce.paymentservice.dto;

import java.math.BigDecimal;

public record PaymentRequest(
        String orderId,
        BigDecimal amount,
        String currency,
        String paymentMethod
) {
}