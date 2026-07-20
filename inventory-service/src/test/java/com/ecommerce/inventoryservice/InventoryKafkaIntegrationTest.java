package com.ecommerce.inventoryservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.ecommerce.events.avro.InventoryReleasedEvent;
import com.ecommerce.events.avro.InventoryReservationFailedEvent;
import com.ecommerce.events.avro.InventoryReservedEvent;
import com.ecommerce.events.avro.OrderCreatedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import com.ecommerce.inventoryservice.entity.Inventory;
import com.ecommerce.inventoryservice.repository.InventoryRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
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
        "spring.kafka.properties.schema.registry.url=mock://inventory-service-it",
        "spring.autoconfigure.exclude="
                + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
@EmbeddedKafka(partitions = 1, topics = {
    "order-created", "inventory-reserved", "inventory-reservation-failed",
    "payment-failed", "inventory-released"
})
class InventoryKafkaIntegrationTest {

    @MockBean
    private InventoryRepository inventoryRepository;

    @org.springframework.beans.factory.annotation.Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, Object> consumer;
    private KafkaTemplate<String, Object> producerTemplate;
    private final Map<String, Inventory> inventoryStore = new HashMap<>();

    @BeforeEach
    void setup() {
        String brokers = embeddedKafkaBroker.getBrokersAsString();

        inventoryStore.clear();
        inventoryStore.put("PRD-200", new Inventory("PRD-200", 10, 0));
        when(inventoryRepository.findById(anyString())).thenAnswer(invocation ->
                Optional.ofNullable(inventoryStore.get(invocation.getArgument(0))));
        when(inventoryRepository.save(any(Inventory.class))).thenAnswer(invocation -> {
            Inventory inventory = invocation.getArgument(0);
            inventoryStore.put(inventory.getProductId(), inventory);
            return inventory;
        });

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-it-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put("schema.registry.url", "mock://inventory-service-it");
        consumerProps.put("specific.avro.reader", true);
        consumer = new DefaultKafkaConsumerFactory<String, Object>(consumerProps).createConsumer();
        consumer.subscribe(List.of("inventory-reserved", "inventory-reservation-failed", "inventory-released"));

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://inventory-service-it");
        producerTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void consumesOrderCreatedAndPublishesInventoryReserved() {
        OrderCreatedEvent orderCreatedEvent = OrderCreatedEvent.newBuilder()
                .setOrderId("ORD-200")
                .setProductId("PRD-200")
                .setQuantity(2)
                .setStatus("CREATED")
                .build();

        producerTemplate.send("order-created", "ORD-200", orderCreatedEvent);

        ConsumerRecord<String, Object> output = pollSingleRecord("inventory-reserved");
        assertThat(output.value()).isInstanceOf(InventoryReservedEvent.class);
        InventoryReservedEvent reservedEvent = (InventoryReservedEvent) output.value();
        assertThat(output.key()).isEqualTo("ORD-200");
        assertThat(reservedEvent.getOrderId().toString()).isEqualTo("ORD-200");
        assertThat(reservedEvent.getReservedQuantity()).isEqualTo(2);
        assertThat(reservedEvent.getStatus().toString()).isEqualTo("RESERVED");
    }

    @Test
    void inventoryFailureFlowPublishesInventoryReservationFailed() {
        OrderCreatedEvent orderCreatedEvent = OrderCreatedEvent.newBuilder()
                .setOrderId("ORD-201")
                .setProductId("PRD-200")
                .setQuantity(50)
                .setStatus("CREATED")
                .build();

        producerTemplate.send("order-created", "ORD-201", orderCreatedEvent);

        ConsumerRecord<String, Object> output = pollSingleRecord("inventory-reservation-failed");
        assertThat(output.value()).isInstanceOf(InventoryReservationFailedEvent.class);
        InventoryReservationFailedEvent failedEvent = (InventoryReservationFailedEvent) output.value();
        assertThat(failedEvent.getOrderId().toString()).isEqualTo("ORD-201");
        assertThat(failedEvent.getStatus().toString()).isEqualTo("FAILED");
    }

    @Test
    void compensationFlowReleasesReservedInventoryOnPaymentFailure() {
        OrderCreatedEvent orderCreatedEvent = OrderCreatedEvent.newBuilder()
                .setOrderId("ORD-202")
                .setProductId("PRD-200")
                .setQuantity(3)
                .setStatus("CREATED")
                .build();
        producerTemplate.send("order-created", "ORD-202", orderCreatedEvent);
        pollSingleRecord("inventory-reserved");

        PaymentFailedEvent paymentFailedEvent = PaymentFailedEvent.newBuilder()
                .setOrderId("ORD-202")
                .setProductId("PRD-200")
                .setReservedQuantity(3)
                .setReason("PAYMENT_DECLINED")
                .setStatus("FAILED")
                .build();
        producerTemplate.send("payment-failed", "ORD-202", paymentFailedEvent);

        ConsumerRecord<String, Object> output = pollSingleRecord("inventory-released");
        assertThat(output.value()).isInstanceOf(InventoryReleasedEvent.class);
        InventoryReleasedEvent releasedEvent = (InventoryReleasedEvent) output.value();
        assertThat(releasedEvent.getOrderId().toString()).isEqualTo("ORD-202");
        assertThat(releasedEvent.getStatus().toString()).isEqualTo("RELEASED");
        assertThat(inventoryStore.get("PRD-200").getReservedQuantity()).isEqualTo(0);
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
