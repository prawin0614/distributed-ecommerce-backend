package com.ecommerce.paymentservice.controller;

import com.ecommerce.paymentservice.dto.PaymentRequest;
import com.ecommerce.paymentservice.entity.Payment;
import com.ecommerce.paymentservice.service.PaymentService;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private static final Logger logger = LoggerFactory.getLogger(PaymentController.class);
    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<?> createPayment(@RequestBody PaymentRequest request) {
        logger.debug("Received create payment request for orderId: {}", request == null ? null : request.orderId());
        try {
            Payment savedPayment = paymentService.createPayment(request);
            logger.info("Payment created successfully: {}", savedPayment.getPaymentId());
            return ResponseEntity.status(HttpStatus.CREATED).body(savedPayment);
        } catch (IllegalArgumentException ex) {
            logger.warn("Invalid payment request: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Error creating payment", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("PAYMENT_CREATION_FAILED", "Failed to create payment: " + ex.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> getAllPayments() {
        logger.debug("Received get all payments request");
        try {
            List<Payment> payments = paymentService.getAllPayments();
            logger.info("Retrieved {} payments", payments.size());
            return ResponseEntity.ok(payments);
        } catch (Exception ex) {
            logger.error("Error retrieving payments", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("PAYMENT_RETRIEVAL_FAILED", "Failed to retrieve payments: " + ex.getMessage()));
        }
    }

    @GetMapping("/{paymentId}")
    public ResponseEntity<?> getPaymentById(@PathVariable Long paymentId) {
        logger.debug("Received get payment request for paymentId: {}", paymentId);
        try {
            return paymentService.getPaymentById(paymentId)
                    .<ResponseEntity<?>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(new ErrorResponse("PAYMENT_NOT_FOUND", "Payment not found for paymentId: " + paymentId)));
        } catch (IllegalArgumentException ex) {
            logger.warn("Invalid payment lookup request: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Error retrieving payment", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("PAYMENT_RETRIEVAL_FAILED", "Failed to retrieve payment: " + ex.getMessage()));
        }
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<?> getPaymentsByOrderId(@PathVariable String orderId) {
        logger.debug("Received get payments by orderId request for orderId: {}", orderId);
        try {
            return ResponseEntity.ok(paymentService.getPaymentsByOrderId(orderId));
        } catch (IllegalArgumentException ex) {
            logger.warn("Invalid payment lookup request: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Error retrieving payments for orderId: {}", orderId, ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("PAYMENT_RETRIEVAL_FAILED", "Failed to retrieve payments: " + ex.getMessage()));
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