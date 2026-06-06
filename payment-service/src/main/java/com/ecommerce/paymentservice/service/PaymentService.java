package com.ecommerce.paymentservice.service;

import com.ecommerce.paymentservice.dto.PaymentRequest;
import com.ecommerce.paymentservice.entity.Payment;
import com.ecommerce.paymentservice.repository.PaymentRepository;
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
}