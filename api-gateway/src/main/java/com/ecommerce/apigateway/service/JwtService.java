package com.ecommerce.apigateway.service;

import com.ecommerce.apigateway.dto.AuthRequest;
import com.ecommerce.apigateway.dto.AuthResponse;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.time.Instant;
import java.util.Date;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class JwtService {
    private final Key signingKey;
    private final long expirationMs;
    private final String allowedUsername;
    private final String allowedPassword;

    public JwtService(
            @Value("${app.jwt.secret}") String secret,
            @Value("${app.jwt.expiration-ms}") long expirationMs,
            @Value("${app.jwt.username}") String allowedUsername,
            @Value("${app.jwt.password}") String allowedPassword
    ) {
        this.signingKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        this.expirationMs = expirationMs;
        this.allowedUsername = allowedUsername;
        this.allowedPassword = allowedPassword;
    }

    public AuthResponse authenticateAndIssueToken(AuthRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Authentication request must not be null");
        }
        if (!allowedUsername.equals(request.username()) || !allowedPassword.equals(request.password())) {
            throw new IllegalArgumentException("Invalid username or password");
        }

        return new AuthResponse(createToken(request.username()), "Bearer", expirationMs / 1000);
    }

    public boolean isTokenValid(String token) {
        try {
            parseClaims(token);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public String extractUsername(String token) {
        return parseClaims(token).getSubject();
    }

    private String createToken(String username) {
        Instant now = Instant.now();
        Instant expiresAt = now.plusMillis(expirationMs);

        return Jwts.builder()
                .setSubject(username)
                .setIssuedAt(Date.from(now))
                .setExpiration(Date.from(expiresAt))
                .signWith(signingKey, SignatureAlgorithm.HS256)
                .compact();
    }

    private Claims parseClaims(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(signingKey)
                .build()
                .parseClaimsJws(token)
                .getBody();
    }
}