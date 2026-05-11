package org.apache.camel.kafkaconnector.nettyhttp.dlq;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceDlqProducer {

    private static final Logger LOG = LoggerFactory.getLogger(SourceDlqProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;

    public SourceDlqProducer(String bootstrapServers, String dlqTopic) {
        this.dlqTopic = dlqTopic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        this.producer = new KafkaProducer<>(props);
        LOG.info("Source DLQ producer initialized, topic: {}", dlqTopic);
    }

    public void send(String rawPayload, String intendedTopic, String eventType,
                     String errorClass, String errorMessage) {
        try {
            Headers headers = new RecordHeaders();
            addHeader(headers, "dlq.error.class", errorClass);
            addHeader(headers, "dlq.error.message", errorMessage);
            addHeader(headers, "dlq.intended.topic", intendedTopic);
            addHeader(headers, "dlq.event.type", eventType);
            addHeader(headers, "dlq.timestamp", String.valueOf(System.currentTimeMillis()));

            ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, null, null,
                    null, rawPayload, headers);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send record to DLQ topic '{}': {}", dlqTopic, exception.getMessage());
                } else {
                    LOG.info("Record sent to DLQ topic '{}' partition {} offset {}",
                            dlqTopic, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            // Last resort — if DLQ producer itself fails, log the full payload
            LOG.error("DLQ producer failed. Original error: [{}] {}. Raw payload: {}",
                    errorClass, errorMessage, rawPayload, e);
        }
    }

    private void addHeader(Headers headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public void close() {
        try {
            producer.flush();
            producer.close();
            LOG.info("Source DLQ producer closed");
        } catch (Exception e) {
            LOG.error("Error closing DLQ producer: {}", e.getMessage());
        }
    }
}