package com.ecommerce.inventoryservice.service;

import com.ecommerce.inventoryservice.dto.InventoryRequest;
import com.ecommerce.inventoryservice.entity.Inventory;
import com.ecommerce.inventoryservice.repository.InventoryRepository;
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
