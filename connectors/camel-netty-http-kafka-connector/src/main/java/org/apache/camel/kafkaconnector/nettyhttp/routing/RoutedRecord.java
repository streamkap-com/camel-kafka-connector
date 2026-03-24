package org.apache.camel.kafkaconnector.nettyhttp.routing;

public class RoutedRecord {
    private final String topic;
    private final String payload;
    private final String eventType;

    public RoutedRecord(String topic, String payload, String eventType) {
        this.topic = topic;
        this.payload = payload;
        this.eventType = eventType;
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

    @Override
    public String toString() {
        return "RoutedRecord{topic='" + topic + "', eventType='" + eventType + "', payloadLength=" + (payload != null ? payload.length() : 0) + "}";
    }
}
