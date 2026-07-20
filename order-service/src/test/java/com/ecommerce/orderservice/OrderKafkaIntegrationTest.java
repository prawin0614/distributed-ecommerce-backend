package com.ecommerce.orderservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.ecommerce.events.avro.InventoryReservationFailedEvent;
import com.ecommerce.events.avro.OrderCancelledEvent;
import com.ecommerce.events.avro.OrderConfirmedEvent;
import com.ecommerce.events.avro.OrderCreatedEvent;
import com.ecommerce.events.avro.PaymentCompletedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import com.ecommerce.orderservice.dto.OrderRequest;
import com.ecommerce.orderservice.entity.Order;
import com.ecommerce.orderservice.repository.OrderRepository;
import com.ecommerce.orderservice.service.OrderService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://order-service-it",
        "spring.autoconfigure.exclude="
                + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
@EmbeddedKafka(partitions = 1, topics = {
    "order-created", "payment-completed", "payment-failed", "inventory-reservation-failed",
    "order-confirmed", "order-cancelled"
})
class OrderKafkaIntegrationTest {

    @Autowired
    private OrderService orderService;

    @MockBean
    private OrderRepository orderRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, Object> consumer;
    private KafkaTemplate<String, Object> producerTemplate;
    private final Map<String, Order> inMemoryOrders = new HashMap<>();

    @BeforeEach
    void setupConsumer() {
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            inMemoryOrders.put(order.getOrderId(), order);
            return order;
        });
        when(orderRepository.findById(anyString())).thenAnswer(invocation ->
                Optional.ofNullable(inMemoryOrders.get(invocation.getArgument(0))));

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-it-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "mock://order-service-it");
        props.put("specific.avro.reader", true);
        consumer = new DefaultKafkaConsumerFactory<String, Object>(props).createConsumer();
        consumer.subscribe(List.of("order-created", "order-confirmed", "order-cancelled"));

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://order-service-it");
        producerTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void successfulOrderFlowConfirmsOrder() {
        orderService.createOrder(new OrderRequest("ORD-100", "PRD-100", 2, "CREATED"));
        ConsumerRecord<String, Object> createdRecord = pollSingleRecord("order-created");
        assertThat(createdRecord.value()).isInstanceOf(OrderCreatedEvent.class);

        PaymentCompletedEvent paymentCompletedEvent = PaymentCompletedEvent.newBuilder()
                .setOrderId("ORD-100")
                .setPaymentId("PMT-100")
                .setStatus("COMPLETED")
                .build();
        producerTemplate.send("payment-completed", "ORD-100", paymentCompletedEvent);

        ConsumerRecord<String, Object> confirmedRecord = pollSingleRecord("order-confirmed");
        assertThat(confirmedRecord.key()).isEqualTo("ORD-100");
        assertThat(confirmedRecord.value()).isInstanceOf(OrderConfirmedEvent.class);
        OrderConfirmedEvent confirmedEvent = (OrderConfirmedEvent) confirmedRecord.value();
        assertThat(confirmedEvent.getStatus().toString()).isEqualTo("CONFIRMED");
        assertThat(inMemoryOrders.get("ORD-100").getStatus()).isEqualTo("CONFIRMED");
    }

    @Test
    void inventoryFailureFlowCancelsOrder() {
        orderService.createOrder(new OrderRequest("ORD-101", "PRD-101", 9, "CREATED"));

        InventoryReservationFailedEvent failedEvent = InventoryReservationFailedEvent.newBuilder()
                .setOrderId("ORD-101")
                .setProductId("PRD-101")
                .setRequestedQuantity(9)
                .setReason("Insufficient inventory")
                .setStatus("FAILED")
                .build();
        producerTemplate.send("inventory-reservation-failed", "ORD-101", failedEvent);

        ConsumerRecord<String, Object> cancelledRecord = pollSingleRecord("order-cancelled");
        assertThat(cancelledRecord.key()).isEqualTo("ORD-101");
        assertThat(cancelledRecord.value()).isInstanceOf(OrderCancelledEvent.class);
        assertThat(inMemoryOrders.get("ORD-101").getStatus()).isEqualTo("CANCELLED");
    }

    @Test
    void paymentFailureFlowMarksOrderFailed() {
        orderService.createOrder(new OrderRequest("ORD-102", "PRD-102", 2, "CREATED"));

        PaymentFailedEvent paymentFailedEvent = PaymentFailedEvent.newBuilder()
                .setOrderId("ORD-102")
                .setProductId("PRD-102")
                .setReservedQuantity(2)
                .setReason("PAYMENT_DECLINED")
                .setStatus("FAILED")
                .build();
        producerTemplate.send("payment-failed", "ORD-102", paymentFailedEvent);

        assertThat(inMemoryOrders.get("ORD-102").getStatus()).isEqualTo("FAILED");
    }

    private ConsumerRecord<String, Object> pollSingleRecord(String topic) {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Object> record : records) {
                if (topic.equals(record.topic())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record found for topic " + topic);
    }
}
