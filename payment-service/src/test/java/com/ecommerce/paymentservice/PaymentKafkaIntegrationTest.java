package com.ecommerce.paymentservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.ecommerce.events.avro.InventoryReservedEvent;
import com.ecommerce.events.avro.PaymentCompletedEvent;
import com.ecommerce.events.avro.PaymentFailedEvent;
import com.ecommerce.paymentservice.entity.Payment;
import com.ecommerce.paymentservice.repository.PaymentRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
        "spring.kafka.properties.schema.registry.url=mock://payment-service-it",
        "spring.autoconfigure.exclude="
                + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
@EmbeddedKafka(partitions = 1, topics = {"inventory-reserved", "payment-completed", "payment-failed"})
class PaymentKafkaIntegrationTest {

    @MockBean
    private PaymentRepository paymentRepository;

    @org.springframework.beans.factory.annotation.Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaTemplate<String, InventoryReservedEvent> producerTemplate;
    private Consumer<String, PaymentCompletedEvent> completedConsumer;
    private Consumer<String, PaymentFailedEvent> failedConsumer;

    @BeforeEach
    void setup() {
        String brokers = embeddedKafkaBroker.getBrokersAsString();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://payment-service-it");
        producerTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        Map<String, Object> completedConsumerProps = new HashMap<>();
        completedConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        completedConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-it-completed-consumer");
        completedConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        completedConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        completedConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        completedConsumerProps.put("schema.registry.url", "mock://payment-service-it");
        completedConsumerProps.put("specific.avro.reader", true);
        completedConsumer = new DefaultKafkaConsumerFactory<String, PaymentCompletedEvent>(completedConsumerProps)
                .createConsumer();
        completedConsumer.subscribe(java.util.List.of("payment-completed"));

        Map<String, Object> failedConsumerProps = new HashMap<>(completedConsumerProps);
        failedConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-it-failed-consumer");
        failedConsumer = new DefaultKafkaConsumerFactory<String, PaymentFailedEvent>(failedConsumerProps)
                .createConsumer();
        failedConsumer.subscribe(java.util.List.of("payment-failed"));
    }

    @AfterEach
    void tearDown() {
        if (completedConsumer != null) {
            completedConsumer.close();
        }
        if (failedConsumer != null) {
            failedConsumer.close();
        }
    }

    @Test
    void consumesInventoryReservedAndPublishesPaymentCompleted() {
        Payment payment = new Payment();
        payment.setPaymentId(9001L);
        payment.setOrderId("ORD-300");
        payment.setAmount(BigDecimal.valueOf(200));
        payment.setCurrency("USD");
        payment.setPaymentMethod("KAFKA_AUTO");
        payment.setStatus("COMPLETED");
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        InventoryReservedEvent input = InventoryReservedEvent.newBuilder()
                .setOrderId("ORD-300")
                .setProductId("PRD-300")
                .setReservedQuantity(2)
                .setStatus("RESERVED")
                .build();

        producerTemplate.send("inventory-reserved", "ORD-300", input);

        ConsumerRecord<String, PaymentCompletedEvent> output = pollCompletedSingleRecord();
        assertThat(output.key()).isEqualTo("ORD-300");
        assertThat(output.value().getOrderId().toString()).isEqualTo("ORD-300");
        assertThat(output.value().getPaymentId().toString()).isEqualTo("9001");
    }

    @Test
    void consumesInventoryReservedAndPublishesPaymentFailed() {
        InventoryReservedEvent input = InventoryReservedEvent.newBuilder()
                .setOrderId("ORD-301")
                .setProductId("PRD-301")
                .setReservedQuantity(8)
                .setStatus("RESERVED")
                .build();

        producerTemplate.send("inventory-reserved", "ORD-301", input);

        ConsumerRecord<String, PaymentFailedEvent> output = pollFailedSingleRecord();
        assertThat(output.key()).isEqualTo("ORD-301");
        assertThat(output.value().getOrderId().toString()).isEqualTo("ORD-301");
        assertThat(output.value().getStatus().toString()).isEqualTo("FAILED");
    }

    private ConsumerRecord<String, PaymentCompletedEvent> pollCompletedSingleRecord() {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentCompletedEvent> records = completedConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, PaymentCompletedEvent> record : records) {
                if ("payment-completed".equals(record.topic())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record found for topic payment-completed");
    }

    private ConsumerRecord<String, PaymentFailedEvent> pollFailedSingleRecord() {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentFailedEvent> records = failedConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, PaymentFailedEvent> record : records) {
                if ("payment-failed".equals(record.topic())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record found for topic payment-failed");
    }
}
