package com.ecommerce.apigateway.dto;

public record AuthResponse(
        String token,
        String tokenType,
        long expiresInSeconds
) {
}