package com.ecommerce.paymentservice.service;

import com.ecommerce.paymentservice.dto.PaymentRequest;
import com.ecommerce.paymentservice.entity.Payment;
import com.ecommerce.paymentservice.repository.PaymentRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import jakarta.transaction.Transactional;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {
    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);
    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public Payment createPayment(PaymentRequest request) {
        validate(request);

        Payment payment = new Payment();
        payment.setOrderId(request.orderId());
        payment.setAmount(request.amount());
        payment.setCurrency(normalizeCurrency(request.currency()));
        payment.setPaymentMethod(request.paymentMethod().trim());
        payment.setStatus("COMPLETED");

        logger.debug("Saving payment entity: {}", payment);
        return paymentRepository.save(payment);
    }

    public List<Payment> getAllPayments() {
        return paymentRepository.findAll();
    }

    public Optional<Payment> getPaymentById(Long paymentId) {
        if (paymentId == null || paymentId <= 0) {
            throw new IllegalArgumentException("paymentId must be greater than 0");
        }
        return paymentRepository.findById(paymentId);
    }

    public List<Payment> getPaymentsByOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        return paymentRepository.findByOrderId(orderId.trim());
    }

    @Retry(name = "paymentOps", fallbackMethod = "paymentProcessingFallback")
    @CircuitBreaker(name = "paymentOps", fallbackMethod = "paymentProcessingFallback")
    @RateLimiter(name = "paymentOps", fallbackMethod = "paymentProcessingFallback")
    public PaymentProcessingResult processPaymentFromInventory(String orderId, int reservedQuantity) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (reservedQuantity <= 0) {
            return new PaymentProcessingResult(orderId, null, false, "Invalid reserved quantity");
        }

        if (reservedQuantity > 5) {
            logger.warn("[SAGA] Payment failed for orderId={} because reservedQuantity={} exceeded allowed threshold",
                    orderId, reservedQuantity);
            return new PaymentProcessingResult(orderId, null, false, "PAYMENT_DECLINED_LIMIT_CHECK");
        }

        Payment payment = new Payment();
        payment.setOrderId(orderId);
        payment.setAmount(BigDecimal.valueOf(reservedQuantity * 100L));
        payment.setCurrency("USD");
        payment.setPaymentMethod("KAFKA_AUTO");
        payment.setStatus("COMPLETED");
        Payment savedPayment = paymentRepository.save(payment);
        logger.info("[SAGA] Payment completed for orderId={}, paymentId={}", orderId, savedPayment.getPaymentId());

        return new PaymentProcessingResult(orderId, String.valueOf(savedPayment.getPaymentId()), true, null);
    }

    @Transactional
    @Retry(name = "paymentOps", fallbackMethod = "refundPaymentFallback")
    @CircuitBreaker(name = "paymentOps", fallbackMethod = "refundPaymentFallback")
    @RateLimiter(name = "paymentOps", fallbackMethod = "refundPaymentFallback")
    public void refundPayment(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }

        Payment payment = paymentRepository.findTopByOrderIdOrderByPaymentIdDesc(orderId)
                .orElse(null);
        if (payment == null) {
            logger.warn("[SAGA] No payment found to refund for orderId={}", orderId);
            return;
        }
        if ("REFUNDED".equalsIgnoreCase(payment.getStatus())) {
            logger.info("[SAGA] Payment already refunded for orderId={}, paymentId={}",
                    orderId, payment.getPaymentId());
            return;
        }

        payment.setStatus("REFUNDED");
        paymentRepository.save(payment);
        logger.info("[SAGA] Payment refunded for orderId={}, paymentId={}",
                orderId, payment.getPaymentId());
    }

    private PaymentProcessingResult paymentProcessingFallback(String orderId, int reservedQuantity, Throwable ex) {
        logger.error("[SAGA] paymentOps fallback triggered for orderId={} reason={}",
                orderId, ex.getMessage(), ex);
        return new PaymentProcessingResult(orderId, null, false, "PAYMENT_SERVICE_UNAVAILABLE");
    }

    private void refundPaymentFallback(String orderId, Throwable ex) {
        logger.error("[SAGA] refund fallback triggered for orderId={} reason={}",
                orderId, ex.getMessage(), ex);
    }

    private void validate(PaymentRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("PaymentRequest must not be null");
        }
        if (request.orderId() == null || request.orderId().isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (request.amount() == null || request.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be greater than 0");
        }
        if (request.currency() == null || request.currency().isBlank()) {
            throw new IllegalArgumentException("currency must not be blank");
        }
        if (request.paymentMethod() == null || request.paymentMethod().isBlank()) {
            throw new IllegalArgumentException("paymentMethod must not be blank");
        }
    }

    private String normalizeCurrency(String currency) {
        return currency.trim().toUpperCase();
    }

    public record PaymentProcessingResult(String orderId, String paymentId, boolean success, String reason) {
    }
}