package com.ecommerce.apigateway.controller;

import com.ecommerce.apigateway.dto.AuthRequest;
import com.ecommerce.apigateway.dto.AuthResponse;
import com.ecommerce.apigateway.service.JwtService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth")
public class AuthController {
    private static final Logger logger = LoggerFactory.getLogger(AuthController.class);
    private final JwtService jwtService;

    public AuthController(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @PostMapping("/token")
    public ResponseEntity<?> issueToken(@RequestBody AuthRequest request) {
        try {
            AuthResponse response = jwtService.authenticateAndIssueToken(request);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException ex) {
            logger.warn("Authentication failed: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(new ErrorResponse("AUTH_FAILED", ex.getMessage()));
        } catch (Exception ex) {
            logger.error("Token issuance failed", ex);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("AUTH_ERROR", "Failed to issue token: " + ex.getMessage()));
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