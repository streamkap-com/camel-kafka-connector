package org.apache.camel.kafkaconnector.nettyhttp.routing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class RoutedRecord {
    private final String topic;
    private final String payload;
    private final String eventType;
    private final Map<String, Object> keyFields;
    private final String op;

    public RoutedRecord(String topic, String payload, String eventType) {
        this(topic, payload, eventType, Collections.emptyMap(), null);
    }

    public RoutedRecord(String topic, String payload, String eventType, Map<String, Object> keyFields) {
        this(topic, payload, eventType, keyFields, null);
    }

    public RoutedRecord(String topic, String payload, String eventType, Map<String, Object> keyFields, String op) {
        this.topic = topic;
        this.payload = payload;
        this.eventType = eventType;
        this.keyFields = keyFields != null ? keyFields : Collections.emptyMap();
        this.op = op;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public String getEventType() {
        return eventType;
    }

    public Map<String, Object> getKeyFields() {
        return keyFields;
    }

    public String getOp() {
        return op;
    }

    public boolean hasKey() {
        return !keyFields.isEmpty();
    }

    @Override
    public String toString() {
        return "RoutedRecord{topic='" + topic + "', eventType='" + eventType
                + "', keyFields=" + keyFields + ", payloadLength=" + (payload != null ? payload.length() : 0) + "}";
    }
}