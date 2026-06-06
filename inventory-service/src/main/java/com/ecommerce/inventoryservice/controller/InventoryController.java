package com.ecommerce.inventoryservice.controller;

import com.ecommerce.inventoryservice.dto.InventoryRequest;
import com.ecommerce.inventoryservice.entity.Inventory;
import com.ecommerce.inventoryservice.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/inventory")
public class InventoryController {
    private static final Logger logger = LoggerFactory.getLogger(InventoryController.class);
    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @PostMapping
    public ResponseEntity<?> createOrUpdateInventory(@RequestBody InventoryRequest request) {
        logger.debug("Received create/update inventory request for product: {}", request.productId());
        try {
            Inventory savedInventory = inventoryService.createOrUpdateInventory(request);
            logger.info("Inventory saved for productId: {}", savedInventory.getProductId());
            return ResponseEntity.status(HttpStatus.CREATED).body(savedInventory);
        } catch (IllegalArgumentException ex) {
            logger.warn("Invalid inventory request: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Error creating/updating inventory", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INVENTORY_OPERATION_FAILED", "Failed to save inventory: " + ex.getMessage()));
        }
    }

    @GetMapping("/{productId}")
    public ResponseEntity<?> getInventory(@PathVariable String productId) {
        logger.debug("Received get inventory request for productId: {}", productId);
        try {
            Optional<Inventory> inventory = inventoryService.getInventory(productId);
            if (inventory.isPresent()) {
                logger.info("Inventory found for productId: {}", productId);
                return ResponseEntity.ok(inventory.get());
            } else {
                logger.warn("Inventory not found for productId: {}", productId);
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(new ErrorResponse("INVENTORY_NOT_FOUND", "Inventory not found for productId: " + productId));
            }
        } catch (Exception ex) {
            logger.error("Error retrieving inventory", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INVENTORY_RETRIEVAL_FAILED", "Failed to retrieve inventory: " + ex.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> getAllInventory() {
        logger.debug("Received get all inventory request");
        try {
            List<Inventory> inventory = inventoryService.getAllInventory();
            logger.info("Retrieved {} inventory items", inventory.size());
            return ResponseEntity.ok(inventory);
        } catch (Exception ex) {
            logger.error("Error retrieving all inventory", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INVENTORY_RETRIEVAL_FAILED", "Failed to retrieve inventory: " + ex.getMessage()));
        }
    }

    @DeleteMapping("/{productId}")
    public ResponseEntity<?> deleteInventory(@PathVariable String productId) {
        logger.debug("Received delete inventory request for productId: {}", productId);
        try {
            inventoryService.deleteInventory(productId);
            logger.info("Inventory deleted for productId: {}", productId);
            return ResponseEntity.noContent().build();
        } catch (Exception ex) {
            logger.error("Error deleting inventory", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INVENTORY_DELETION_FAILED", "Failed to delete inventory: " + ex.getMessage()));
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
