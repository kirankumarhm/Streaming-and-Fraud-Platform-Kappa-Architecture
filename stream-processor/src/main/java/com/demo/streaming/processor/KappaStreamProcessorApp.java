package com.demo.streaming.processor;

import com.demo.streaming.contracts.AccountMetricsWindow;
import com.demo.streaming.contracts.FraudAlert;
import com.demo.streaming.contracts.PaymentTransaction;
import com.demo.streaming.processor.exception.PoisonPillException;
import com.demo.streaming.processor.strategy.FraudRuleRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Core Kappa Stream Processor entrypoint.
 * Demonstrates single-codebase streaming analytics, stateful fraud anomaly detection,
 * dead letter queue routing, and historical log replay capabilities.
 */
public class KappaStreamProcessorApp {
    private static final Logger log = LoggerFactory.getLogger(KappaStreamProcessorApp.class);

    private static final String INPUT_TOPIC = "payment.transactions.v1";
    private static final String DLQ_TOPIC = "payment.transactions.dlq";
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    public static void main(String[] args) {
        boolean replayMode = Arrays.asList(args).contains("--replay")
                || Boolean.parseBoolean(System.getenv().getOrDefault("REPLAY_MODE", "false"));

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String jdbcUrl = System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/streaming_db");
        String dbUser = System.getenv().getOrDefault("DB_USER", "postgres");
        String dbPass = System.getenv().getOrDefault("DB_PASS", "postgrespassword");

        // When replaying in Kappa architecture, an ephemeral group is used to rewind to earliest offset
        String groupId = replayMode
                ? "kappa-replay-processor-" + System.currentTimeMillis()
                : "kappa-payment-processor-v1";

        log.info("==========================================================");
        log.info("Starting Enterprise Kappa Stream Processor");
        log.info("Execution Mode: {}", replayMode ? "HISTORICAL REPLAY (Offset 0)" : "REAL-TIME STREAMING");
        log.info("Consumer Group: {}", groupId);
        log.info("Broker Endpoint: {}", bootstrapServers);
        log.info("Serving Database: {}", jdbcUrl);
        log.info("==========================================================");

        DbSink dbSink = new DbSink(jdbcUrl, dbUser, dbPass);
        FraudRuleRegistry fraudRegistry = new FraudRuleRegistry();
        WindowAggregator windowAggregator = new WindowAggregator();

        // DLQ Producer initialization
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(producerProps);

        // Consumer initialization
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, replayMode ? "earliest" : "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual commit for at-least-once guarantee
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        AtomicBoolean isRunning = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Signal received. Initiating graceful shutdown of Kappa processor...");
            isRunning.set(false);
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(List.of(INPUT_TOPIC));
            log.info("Subscribed to input topic: {}", INPUT_TOPIC);

            long totalProcessed = 0;
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        PaymentTransaction tx = deserializePayload(record);
                        MDC.put("txId", tx.transactionId());
                        MDC.put("accountId", tx.accountId());

                        // Step 1: Stateful Tumbling Window Aggregation
                        AccountMetricsWindow metricsSnapshot = windowAggregator.process(tx);
                        dbSink.upsertAccountMetrics(metricsSnapshot);

                        // Step 2: Stateful Fraud Strategy Evaluation
                        List<FraudAlert> alerts = fraudRegistry.evaluateAll(tx);
                        for (FraudAlert alert : alerts) {
                            dbSink.insertFraudAlert(alert);
                        }

                        totalProcessed++;
                        if (totalProcessed % 50 == 0) {
                            log.info("Stream progress: {} events processed. Last window total for {}: ${}",
                                    totalProcessed, metricsSnapshot.accountId(), metricsSnapshot.totalAmount());
                        }

                    } catch (PoisonPillException ppe) {
                        log.error("Routing poisoned message to DLQ [{}]: {}", DLQ_TOPIC, ppe.getMessage());
                        dlqProducer.send(new ProducerRecord<>(DLQ_TOPIC, record.key(), record.value()));
                    } catch (Exception ex) {
                        log.error("Unexpected processing error at offset {}", record.offset(), ex);
                    } finally {
                        MDC.clear();
                    }
                }

                // Synchronous offset commit after database batch write
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                // Cleanup inactive windows
                windowAggregator.cleanupOldWindows();
                fraudRegistry.cleanupInactiveAccounts();
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (isRunning.get()) throw e;
        } finally {
            consumer.close();
            dlqProducer.close();
            dbSink.close();
            log.info("Stream Processor shut down completely.");
        }
    }

    private static PaymentTransaction deserializePayload(ConsumerRecord<String, String> record) {
        try {
            return MAPPER.readValue(record.value(), PaymentTransaction.class);
        } catch (Exception e) {
            throw new PoisonPillException(record.value(), record.offset(), "Deserialization failure", e);
        }
    }
}
