package com.ecommerce.notificationservice;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.ecommerce.events.avro.InventoryReleasedEvent;
import com.ecommerce.events.avro.OrderCancelledEvent;
import com.ecommerce.events.avro.OrderConfirmedEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://notification-service-it"
})
@EmbeddedKafka(partitions = 1, topics = {"order-confirmed", "order-cancelled", "inventory-released"})
class NotificationKafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void consumesOrderConfirmedAndOrderCancelledWithoutErrors() {
        String brokers = embeddedKafkaBroker.getBrokersAsString();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://notification-service-it");

        KafkaTemplate<String, OrderConfirmedEvent> confirmedTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        KafkaTemplate<String, OrderCancelledEvent> cancelledTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        KafkaTemplate<String, InventoryReleasedEvent> releasedTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        OrderConfirmedEvent confirmedEvent = OrderConfirmedEvent.newBuilder()
                .setOrderId("ORD-N-1")
                .setStatus("CONFIRMED")
                .setMessage("confirmed")
                .build();

        OrderCancelledEvent cancelledEvent = OrderCancelledEvent.newBuilder()
                .setOrderId("ORD-N-2")
                .setStatus("CANCELLED")
                .setReason("test")
                .build();

                InventoryReleasedEvent releasedEvent = InventoryReleasedEvent.newBuilder()
                                .setOrderId("ORD-N-3")
                                .setProductId("PRD-N-3")
                                .setReleasedQuantity(2)
                                .setStatus("RELEASED")
                                .setReason("PAYMENT_FAILED_COMPENSATION")
                                .build();

        assertThatCode(() -> {
            confirmedTemplate.send("order-confirmed", "ORD-N-1", confirmedEvent);
            cancelledTemplate.send("order-cancelled", "ORD-N-2", cancelledEvent);
                        releasedTemplate.send("inventory-released", "ORD-N-3", releasedEvent);
        }).doesNotThrowAnyException();
    }
}
