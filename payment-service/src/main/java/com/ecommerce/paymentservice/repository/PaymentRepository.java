package com.ecommerce.paymentservice.repository;

import com.ecommerce.paymentservice.entity.Payment;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
    List<Payment> findByOrderId(String orderId);
    Optional<Payment> findTopByOrderIdOrderByPaymentIdDesc(String orderId);
}