package com.ecommerce.apigateway.dto;

public record AuthRequest(
        String username,
        String password
) {
}