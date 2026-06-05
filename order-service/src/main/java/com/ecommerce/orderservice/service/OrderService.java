package com.ecommerce.orderservice.service;

import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.event.OrderCreatedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderService {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String orderCreatedTopic;

    public OrderService(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
            @Value("${app.kafka.order-created-topic}") String orderCreatedTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
    }

    public String createOrder(OrderRequest request) {
        String status = request.status();
        if (status == null || status.isBlank()) {
            status = "CREATED";
        }

        OrderCreatedEvent event = new OrderCreatedEvent(
                request.orderId(),
                request.productId(),
                request.quantity(),
                status
        );

        kafkaTemplate.send(orderCreatedTopic, event);
        return "Order created and event sent to Kafka";
    }
}
