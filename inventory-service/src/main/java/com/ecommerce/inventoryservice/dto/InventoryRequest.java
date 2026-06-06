package com.ecommerce.inventoryservice.dto;

public record InventoryRequest(
        String productId,
        int availableQuantity,
        int reservedQuantity
) {
}
