# Week 2 - Event-Driven Setup (Spring Boot 3, Java 17)

## Architecture
Order Service -> Kafka -> Inventory Service -> Kafka -> Payment Service

## Kafka Infrastructure
Use the Compose stack in [docker-compose.kafka.yml](docker-compose.kafka.yml):

- Kafka broker
- Schema Registry
- Kafka UI (optional)
- Topic initializer container

Created topics:

- order-created
- inventory-reserved
- payment-completed
- payment-failed

Run:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Kafka UI:

- http://localhost:8086

Schema Registry:

- http://localhost:8085

## Avro Schemas
Order Service:

- order-service/src/main/avro/OrderCreatedEvent.avsc

Inventory Service:

- inventory-service/src/main/avro/OrderCreatedEvent.avsc
- inventory-service/src/main/avro/InventoryReservedEvent.avsc

Payment Service:

- payment-service/src/main/avro/InventoryReservedEvent.avsc
- payment-service/src/main/avro/PaymentCompletedEvent.avsc
- payment-service/src/main/avro/PaymentFailedEvent.avsc

Java classes are generated using avro-maven-plugin during generate-sources.

## Spring Kafka and Consumer Groups
Per service, application configuration includes:

- Avro serializer/deserializer
- schema.registry.url
- specific.avro.reader
- consumer group configuration
- topic names under app.kafka.topics

Consumer groups used:

- inventory-group
- payment-group

## Retry and Error Handling
Inventory Service and Payment Service configure DefaultErrorHandler with:

- 2 retries
- 2 second backoff
- retry logging for each failed attempt

## Integration Tests
Added integration tests:

- order-service/src/test/java/com/ecommerce/orderservice/OrderKafkaIntegrationTest.java
- inventory-service/src/test/java/com/ecommerce/inventoryservice/InventoryKafkaIntegrationTest.java
- payment-service/src/test/java/com/ecommerce/paymentservice/PaymentKafkaIntegrationTest.java

These tests validate the event chain behavior and message keys (orderId).
