# Distributed E-Commerce Microservices (Event-Driven)

A scalable event-driven e-commerce backend built using Java Spring Boot microservices. The system follows a microservices architecture with Apache Kafka for asynchronous communication and the Saga pattern for distributed transaction management.

## Project Overview

This project decomposes an e-commerce platform into independently deployable services.

The services communicate through Apache Kafka events instead of direct synchronous calls, improving scalability, reliability, and fault tolerance.

---

## Architecture

- API Gateway
- Eureka Service Registry
- Spring Cloud Config Server
- Order Service
- Inventory Service
- Payment Service
- Notification Service
- Apache Kafka
- PostgreSQL
- MongoDB (prepared for product catalog)
- Docker

---

## Technology Stack

### Backend
- Java 17
- Spring Boot 3
- Spring Cloud
- Spring Security (JWT)
- Spring Data JPA

### Messaging
- Apache Kafka
- Apache Avro
- Confluent Schema Registry

### Database
- PostgreSQL
- MongoDB

### Service Discovery
- Netflix Eureka

### API Gateway
- Spring Cloud Gateway

### Fault Tolerance
- Resilience4j
  - Circuit Breaker
  - Retry
  - Rate Limiter

### Build Tools
- Maven
- Docker

---

# Implemented Features

## Week 1

✔ Service Registry (Eureka)

✔ Config Server

✔ API Gateway

✔ JWT Authentication

✔ Order Service

✔ Inventory Service

✔ Payment Service

✔ Notification Service

✔ PostgreSQL Integration

---

## Week 2

✔ Apache Kafka Integration

✔ Event Driven Architecture

✔ Kafka Producers

✔ Kafka Consumers

✔ Avro Serialization

✔ Schema Registry

✔ Kafka Topics

- order-created
- inventory-reserved
- payment-completed
- payment-failed

---

## Week 3

✔ Choreography Saga Pattern

✔ Distributed Transactions

✔ Compensation Logic

✔ Inventory Reservation

✔ Payment Processing

✔ Payment Failure Recovery

✔ Order Status Management

✔ Resilience4j

- Circuit Breaker
- Retry
- Rate Limiter

---

## Current Project Flow

Customer

↓

API Gateway

↓

Order Service

↓

Kafka

↓

Inventory Service

↓

Kafka

↓

Payment Service

↓

Kafka

↓

Notification Service

---

## Microservices

### API Gateway
- JWT Authentication
- Routing
- Request Filtering

Port:
```
8080
```

---

### Service Registry

Port

```
8761
```

---

### Config Server

Port

```
8888
```

---

### Order Service

Port

```
8081
```

Responsibilities

- Create Orders
- Publish Kafka Events
- Handle Saga Events
- Maintain Order Status

---

### Inventory Service

Port

```
8082
```

Responsibilities

- Reserve Inventory
- Release Inventory
- Publish Inventory Events

---

### Payment Service

Port

```
8083
```

Responsibilities

- Process Payment
- Publish Payment Success
- Publish Payment Failure

---

### Notification Service

Port

```
8084
```

Responsibilities

- Consume Kafka Events
- Send Notifications (Simulation)

---

# Kafka Topics

```
order-created

inventory-reserved

payment-completed

payment-failed
```

---

# Running the Project

## Clone Repository

```bash
git clone https://github.com/prawin0614/distributed-ecommerce-backend.git
```

---

## Build

```bash
mvn clean install
```

---

## Start Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d
```

---

## Start Services

1. Service Registry

```bash
mvn spring-boot:run
```

2. Config Server

```bash
mvn spring-boot:run
```

3. Order Service

```bash
mvn spring-boot:run
```

4. Inventory Service

```bash
mvn spring-boot:run
```

5. Payment Service

```bash
mvn spring-boot:run
```

6. Notification Service

```bash
mvn spring-boot:run
```

7. API Gateway

```bash
mvn spring-boot:run
```

---

# API Endpoints

## Create Order

POST

```
http://localhost:8080/orders
```

Example

```json
{
  "orderId":"ORD-1001",
  "productId":"PRD-100",
  "quantity":2,
  "status":"CREATED"
}
```

---

## Add Inventory

POST

```
http://localhost:8080/inventory
```

```json
{
  "productId":"PRD-100",
  "availableQuantity":20,
  "reservedQuantity":0
}
```

---

## Create Payment

POST

```
http://localhost:8080/payments
```

```json
{
  "orderId":"ORD-1001",
  "amount":500,
  "currency":"INR",
  "paymentMethod":"UPI",
  "status":"SUCCESS"
}
```

---

# Project Status

| Module | Status |
|---------|--------|
| Week 1 | ✅ Completed |
| Week 2 | ✅ Completed |
| Week 3 | ✅ Completed |
| Week 4 | 🔄 In Progress (Observability & Kubernetes) |

---

# Future Enhancements

- Kubernetes Deployment
- Prometheus Metrics
- Zipkin Distributed Tracing
- Grafana Dashboard
- CI/CD Pipeline
- Helm Charts
- Monitoring Dashboard

---

# Author

**Prawin A**

BCA Graduate

Java Full Stack Developer

GitHub

https://github.com/prawin0614
