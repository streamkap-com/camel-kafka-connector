package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka consumer that reads snapshot signal commands from a signal topic.
 * Signals trigger incremental or blocking snapshots on demand.
 */
public class SnapshotSignalConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSignalConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;

    public SnapshotSignalConsumer(String bootstrapServers, String signalTopic, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(signalTopic));
        this.objectMapper = new ObjectMapper();

        LOG.info("Snapshot signal consumer initialized, topic: {}, group: {}", signalTopic, groupId);
    }

    @SuppressWarnings("unchecked")
    public List<SnapshotSignal> poll(Duration timeout) {
        List<SnapshotSignal> signals = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(timeout);

            for (ConsumerRecord<String, String> record : records) {
                try {
                    Map<String, Object> signalData = objectMapper.readValue(record.value(), Map.class);
                    SnapshotSignal signal = parseSignal(signalData);
                    if (signal != null) {
                        signals.add(signal);
                        LOG.info("Received snapshot signal: {}", signal);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to parse snapshot signal: {}", record.value(), e);
                }
            }
        } catch (Exception e) {
            LOG.error("Error polling snapshot signals: {}", e.getMessage());
        }

        return signals;
    }

    @SuppressWarnings("unchecked")
    private SnapshotSignal parseSignal(Map<String, Object> signalData) {
        String id = (String) signalData.get("id");
        String type = (String) signalData.get("type");

        if (type == null) {
            LOG.warn("Signal missing 'type' field, ignoring: {}", signalData);
            return null;
        }

        Map<String, Object> data = (Map<String, Object>) signalData.getOrDefault("data", Collections.emptyMap());

        return new SnapshotSignal(id, type, data);
    }

    public void close() {
        try {
            consumer.close();
            LOG.info("Snapshot signal consumer closed");
        } catch (Exception e) {
            LOG.error("Error closing signal consumer: {}", e.getMessage());
        }
    }

    /**
     * Parsed snapshot signal.
     */
    public static class SnapshotSignal {
        private final String id;
        private final String type;
        private final Map<String, Object> data;

        public SnapshotSignal(String id, String type, Map<String, Object> data) {
            this.id = id;
            this.type = type;
            this.data = data;
        }

        public String getId() { return id; }
        public String getType() { return type; }
        public Map<String, Object> getData() { return data; }

        @SuppressWarnings("unchecked")
        public List<String> getObjects() {
            Object objects = data.get("objects");
            if (objects instanceof List) {
                return (List<String>) objects;
            }
            return Collections.emptyList();
        }

        public SnapshotType getSnapshotType() {
            String typeStr = (String) data.getOrDefault("snapshot_type", "INCREMENTAL");
            try {
                return SnapshotType.valueOf(typeStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                return SnapshotType.INCREMENTAL;
            }
        }

        public int getChunkSize(int defaultSize) {
            Object size = data.get("chunk_size");
            if (size instanceof Number) {
                return ((Number) size).intValue();
            }
            return defaultSize;
        }

        public String getAdditionalCondition() {
            return (String) data.get("additional_condition");
        }

        @Override
        public String toString() {
            return "SnapshotSignal{id='" + id + "', type='" + type + "', objects=" + getObjects()
                    + ", snapshotType=" + getSnapshotType() + "}";
        }
    }
}
