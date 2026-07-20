package com.ecommerce.inventoryservice.service;

import com.ecommerce.inventoryservice.dto.InventoryRequest;
import com.ecommerce.inventoryservice.entity.Inventory;
import com.ecommerce.inventoryservice.repository.InventoryRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
public class InventoryService {
    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);
    private final InventoryRepository inventoryRepository;

    public InventoryService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    public Inventory createOrUpdateInventory(InventoryRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("InventoryRequest must not be null");
        }
        if (request.productId() == null || request.productId().isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (request.availableQuantity() < 0) {
            throw new IllegalArgumentException("availableQuantity must not be negative");
        }

        logger.debug("Creating/Updating inventory for product: {}", request.productId());
        
        Inventory inventory = new Inventory();
        inventory.setProductId(request.productId());
        inventory.setAvailableQuantity(request.availableQuantity());
        inventory.setReservedQuantity(request.reservedQuantity() >= 0 ? request.reservedQuantity() : 0);

        Inventory savedInventory = inventoryRepository.save(inventory);
        logger.info("Inventory saved for productId: {}", savedInventory.getProductId());
        return savedInventory;
    }

    public Optional<Inventory> getInventory(String productId) {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        logger.debug("Fetching inventory for product: {}", productId);
        return inventoryRepository.findById(productId);
    }

    public List<Inventory> getAllInventory() {
        logger.debug("Fetching all inventory items");
        try {
            List<Inventory> items = inventoryRepository.findAll();
            logger.debug("Retrieved {} inventory items", items.size());
            return items;
        } catch (Exception ex) {
            logger.error("Error retrieving inventory items", ex);
            throw new RuntimeException("Failed to retrieve inventory: " + ex.getMessage(), ex);
        }
    }

    @Transactional
    @Retry(name = "inventoryOps", fallbackMethod = "reserveInventoryFallback")
    @CircuitBreaker(name = "inventoryOps", fallbackMethod = "reserveInventoryFallback")
    @RateLimiter(name = "inventoryOps", fallbackMethod = "reserveInventoryFallback")
    public void reserveInventory(String productId, int quantity) {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be greater than 0");
        }

        Inventory inventory = inventoryRepository.findById(productId)
                .orElseThrow(() -> new IllegalStateException("No inventory record found for productId: " + productId));

        if (inventory.getAvailableQuantity() < quantity) {
            throw new IllegalStateException("Insufficient inventory for productId: " + productId);
        }

        inventory.setAvailableQuantity(inventory.getAvailableQuantity() - quantity);
        inventory.setReservedQuantity(inventory.getReservedQuantity() + quantity);
        inventoryRepository.save(inventory);
        logger.info("Reserved inventory for productId={}, quantity={}", productId, quantity);
    }

    @Transactional
    @Retry(name = "inventoryOps", fallbackMethod = "releaseInventoryFallback")
    @CircuitBreaker(name = "inventoryOps", fallbackMethod = "releaseInventoryFallback")
    @RateLimiter(name = "inventoryOps", fallbackMethod = "releaseInventoryFallback")
    public void releaseReservedInventory(String productId, int quantity) {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be greater than 0");
        }

        Inventory inventory = inventoryRepository.findById(productId)
                .orElseThrow(() -> new IllegalStateException("No inventory record found for productId: " + productId));

        int releasable = Math.min(quantity, inventory.getReservedQuantity());
        inventory.setReservedQuantity(inventory.getReservedQuantity() - releasable);
        inventory.setAvailableQuantity(inventory.getAvailableQuantity() + releasable);
        inventoryRepository.save(inventory);

        logger.info("[SAGA] Inventory released for productId={}, quantity={}", productId, releasable);
    }

    private void reserveInventoryFallback(String productId, int quantity, Throwable ex) {
        logger.error("[SAGA] reserveInventory fallback productId={} quantity={} reason={}",
                productId, quantity, ex.getMessage(), ex);
        throw new IllegalStateException("Inventory reservation fallback triggered for productId: " + productId, ex);
    }

    private void releaseInventoryFallback(String productId, int quantity, Throwable ex) {
        logger.error("[SAGA] releaseReservedInventory fallback productId={} quantity={} reason={}",
                productId, quantity, ex.getMessage(), ex);
        throw new IllegalStateException("Inventory release fallback triggered for productId: " + productId, ex);
    }

    public boolean deleteInventory(String productId) {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        logger.debug("Deleting inventory for product: {}", productId);
        try {
            inventoryRepository.deleteById(productId);
            logger.info("Inventory deleted for productId: {}", productId);
            return true;
        } catch (Exception ex) {
            logger.error("Error deleting inventory for productId: {}", productId, ex);
            throw new RuntimeException("Failed to delete inventory: " + ex.getMessage(), ex);
        }
    }
}
