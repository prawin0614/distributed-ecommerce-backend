package com.ecommerce.inventoryservice.repository;

import com.ecommerce.inventoryservice.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryRepository extends JpaRepository<Inventory, String> {
}
