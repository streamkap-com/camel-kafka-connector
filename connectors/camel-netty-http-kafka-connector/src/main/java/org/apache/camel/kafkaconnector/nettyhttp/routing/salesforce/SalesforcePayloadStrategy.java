package org.apache.camel.kafkaconnector.nettyhttp.routing.salesforce;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesforcePayloadStrategy implements PayloadRoutingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforcePayloadStrategy.class);

    private static final String DELETED_FIELD = "__deleted";
    private static final String CHANGE_EVENT_HEADER = "ChangeEventHeader";

    private String topicPrefix = "";
    private UnknownTypeBehavior unknownTypeBehavior = UnknownTypeBehavior.DEFAULT_TOPIC;
    private String defaultTopic = "unknown";
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Advanced config
    private boolean flattenDetail = false;
    private String flattenDetailPrefix = "";
    private boolean includeEvent = true;

    @Override
    public void configure(String topicPrefix, UnknownTypeBehavior unknownTypeBehavior, String defaultTopic) {
        this.topicPrefix = topicPrefix != null ? topicPrefix : "";
        this.unknownTypeBehavior = unknownTypeBehavior != null ? unknownTypeBehavior : UnknownTypeBehavior.DEFAULT_TOPIC;
        this.defaultTopic = defaultTopic != null ? defaultTopic : "unknown";
    }

    @Override
    public void configureAdvanced(Set<String> fanoutFields, boolean flattenDetail,
                                  String flattenDetailPrefix, boolean includeEvent) {
        this.flattenDetail = flattenDetail;
        this.flattenDetailPrefix = flattenDetailPrefix != null ? flattenDetailPrefix : "";
        this.includeEvent = includeEvent;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(Map<String, Object> payload) {
        // Detect event format
        Map<String, Object> data = getMapField(payload, "data");

        if (data != null) {
            Map<String, Object> cdcPayload = getMapField(data, "payload");
            if (cdcPayload != null && cdcPayload.containsKey(CHANGE_EVENT_HEADER)) {
                return routeCdcEvent(payload, data, cdcPayload);
            }

            Map<String, Object> sobject = getMapField(data, "sobject");
            if (sobject != null) {
                return routePushTopicEvent(payload, data, sobject);
            }

            // Platform event: has data.payload but no ChangeEventHeader
            if (cdcPayload != null) {
                return routePlatformEvent(payload, data, cdcPayload);
            }
        }

        return handleUnknownType(payload, "<unrecognized>");
    }

    // --- Change Data Capture Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routeCdcEvent(Map<String, Object> payload,
                                              Map<String, Object> data,
                                              Map<String, Object> cdcPayload) {
        Map<String, Object> header = getMapField(cdcPayload, CHANGE_EVENT_HEADER);
        if (header == null) {
            throw new PayloadRoutingException("Salesforce CDC event missing ChangeEventHeader");
        }

        String entityName = (String) header.get("entityName");
        if (entityName == null || entityName.isEmpty()) {
            throw new PayloadRoutingException("Salesforce CDC event missing entityName in ChangeEventHeader");
        }

        String changeType = (String) header.get("changeType");
        if (changeType == null) {
            changeType = "UNKNOWN";
        }

        // Handle GAP events — route to separate topic
        if (changeType.startsWith("GAP_")) {
            return routeGapEvent(payload, data, cdcPayload, header, entityName, changeType);
        }

        // Extract record ID
        Object recordId = extractCdcRecordId(cdcPayload, header);
        String eventType = entityName + "." + changeType;

        // Build output payload
        Map<String, Object> outputPayload = buildCdcPayload(cdcPayload, data, header, changeType);

        String topicSuffix = entityName.toLowerCase() + "_events";
        String idField = flattenDetail ? flattenDetailPrefix + "Id" : "Id";
        Map<String, Object> key = recordId != null ? keyOf(idField, recordId) : Collections.emptyMap();

        return Collections.singletonList(createRecord(topicSuffix, outputPayload, eventType, key));
    }

    private List<RoutedRecord> routeGapEvent(Map<String, Object> payload,
                                              Map<String, Object> data,
                                              Map<String, Object> cdcPayload,
                                              Map<String, Object> header,
                                              String entityName, String changeType) {
        Object recordId = extractCdcRecordId(cdcPayload, header);
        String eventType = entityName + "." + changeType;

        Map<String, Object> outputPayload = new LinkedHashMap<>();
        outputPayload.put("entityName", entityName);
        outputPayload.put("changeType", changeType);
        if (recordId != null) {
            outputPayload.put("Id", recordId);
        }
        outputPayload.put("_gap", true);
        outputPayload.putAll(header);
        outputPayload.put(DELETED_FIELD, isDeleteChangeType(changeType));

        Map<String, Object> key = recordId != null ? keyOf("Id", recordId) : Collections.emptyMap();

        return Collections.singletonList(createRecord("gap_events", outputPayload, eventType, key));
    }

    @SuppressWarnings("unchecked")
    private Object extractCdcRecordId(Map<String, Object> cdcPayload, Map<String, Object> header) {
        // Try payload.Id first
        Object id = cdcPayload.get("Id");
        if (id != null) {
            return id;
        }
        // Fall back to recordIds[0]
        Object recordIds = header.get("recordIds");
        if (recordIds instanceof java.util.List) {
            java.util.List<?> ids = (java.util.List<?>) recordIds;
            if (!ids.isEmpty()) {
                return ids.get(0);
            }
        }
        return null;
    }

    private Map<String, Object> buildCdcPayload(Map<String, Object> cdcPayload,
                                                  Map<String, Object> data,
                                                  Map<String, Object> header,
                                                  String changeType) {
        boolean isDeleted = isDeleteChangeType(changeType);

        if (flattenDetail) {
            // Flatten: promote payload fields to top level, add metadata
            Map<String, Object> result = new LinkedHashMap<>();

            for (Map.Entry<String, Object> entry : cdcPayload.entrySet()) {
                if (CHANGE_EVENT_HEADER.equals(entry.getKey())) {
                    continue;
                }
                result.put(flattenDetailPrefix + entry.getKey(), entry.getValue());
            }

            // Add metadata from header
            if (includeEvent) {
                result.put("changeType", changeType);
                result.put("entityName", header.get("entityName"));
                if (header.get("commitTimestamp") != null) {
                    result.put("commitTimestamp", header.get("commitTimestamp"));
                }
                if (header.get("commitUser") != null) {
                    result.put("commitUser", header.get("commitUser"));
                }
                if (header.get("transactionKey") != null) {
                    result.put("transactionKey", header.get("transactionKey"));
                }
            }

            // Add replay ID
            Map<String, Object> event = getMapField(data, "event");
            if (event != null && event.get("replayId") != null) {
                result.put("replayId", event.get("replayId"));
            }

            result.put(DELETED_FIELD, isDeleted);
            return result;
        }

        // Non-flattened: keep structure but add __deleted and optionally strip header
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : cdcPayload.entrySet()) {
            if (!includeEvent && CHANGE_EVENT_HEADER.equals(entry.getKey())) {
                continue;
            }
            result.put(entry.getKey(), entry.getValue());
        }

        // Add replay ID
        Map<String, Object> event = getMapField(data, "event");
        if (event != null && event.get("replayId") != null) {
            result.put("replayId", event.get("replayId"));
        }

        result.put(DELETED_FIELD, isDeleted);
        return result;
    }

    // --- PushTopic Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routePushTopicEvent(Map<String, Object> payload,
                                                    Map<String, Object> data,
                                                    Map<String, Object> sobject) {
        // Extract object type from channel: /topic/AccountPushTopic
        String channel = (String) payload.get("channel");
        String objectType = parsePushTopicChannel(channel);

        // Extract operation from data.event.type
        Map<String, Object> event = getMapField(data, "event");
        String operationType = "unknown";
        if (event != null && event.get("type") != null) {
            operationType = String.valueOf(event.get("type"));
        }

        Object recordId = sobject.get("Id");
        String eventType = objectType + "." + operationType;

        boolean isDeleted = "deleted".equals(operationType);

        // Build output
        Map<String, Object> outputPayload = new LinkedHashMap<>();

        if (flattenDetail) {
            for (Map.Entry<String, Object> entry : sobject.entrySet()) {
                outputPayload.put(flattenDetailPrefix + entry.getKey(), entry.getValue());
            }
        } else {
            outputPayload.putAll(sobject);
        }

        if (includeEvent && event != null) {
            outputPayload.put("operationType", operationType);
            if (event.get("createdDate") != null) {
                outputPayload.put("eventCreatedDate", event.get("createdDate"));
            }
            if (event.get("replayId") != null) {
                outputPayload.put("replayId", event.get("replayId"));
            }
        }

        outputPayload.put(DELETED_FIELD, isDeleted);

        String topicSuffix = objectType.toLowerCase() + "_events";
        String idField = flattenDetail ? flattenDetailPrefix + "Id" : "Id";
        Map<String, Object> key = recordId != null ? keyOf(idField, recordId) : Collections.emptyMap();

        return Collections.singletonList(createRecord(topicSuffix, outputPayload, eventType, key));
    }

    private String parsePushTopicChannel(String channel) {
        if (channel == null || channel.isEmpty()) {
            return "unknown";
        }
        // /topic/AccountPushTopic → AccountPushTopic
        // /topic/MyAccountStream → MyAccountStream
        int lastSlash = channel.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < channel.length() - 1) {
            return channel.substring(lastSlash + 1);
        }
        return channel;
    }

    // --- Platform Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routePlatformEvent(Map<String, Object> payload,
                                                   Map<String, Object> data,
                                                   Map<String, Object> eventPayload) {
        // Extract event name from channel: /event/Low_Ink__e → Low_Ink__e
        String channel = (String) payload.get("channel");
        String eventName = parsePlatformEventChannel(channel);

        Object recordId = eventPayload.get("Id");
        String eventType = eventName + ".published";

        // Build output
        Map<String, Object> outputPayload = new LinkedHashMap<>();

        if (flattenDetail) {
            for (Map.Entry<String, Object> entry : eventPayload.entrySet()) {
                outputPayload.put(flattenDetailPrefix + entry.getKey(), entry.getValue());
            }
        } else {
            outputPayload.putAll(eventPayload);
        }

        // Add replay ID
        Map<String, Object> event = getMapField(data, "event");
        if (event != null && event.get("replayId") != null) {
            outputPayload.put("replayId", event.get("replayId"));
        }

        // Platform events are never deletes
        outputPayload.put(DELETED_FIELD, false);

        String topicSuffix = eventName.toLowerCase() + "_events";
        String idField = flattenDetail ? flattenDetailPrefix + "Id" : "Id";
        Map<String, Object> key = recordId != null ? keyOf(idField, recordId) : Collections.emptyMap();

        return Collections.singletonList(createRecord(topicSuffix, outputPayload, eventType, key));
    }

    private String parsePlatformEventChannel(String channel) {
        if (channel == null || channel.isEmpty()) {
            return "unknown";
        }
        // /event/Low_Ink__e → Low_Ink__e
        int lastSlash = channel.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < channel.length() - 1) {
            return channel.substring(lastSlash + 1);
        }
        return channel;
    }

    // --- Delete Detection ---

    private boolean isDeleteChangeType(String changeType) {
        if (changeType == null) {
            return false;
        }
        // DELETE, GAP_DELETE are deletes
        // UNDELETE, GAP_UNDELETE are restores
        return "DELETE".equals(changeType) || "GAP_DELETE".equals(changeType);
    }

    // --- Key Helpers ---

    private Map<String, Object> keyOf(String field, Object value) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(field, value);
        return key;
    }

    // --- Helpers ---

    private List<RoutedRecord> handleUnknownType(Map<String, Object> payload, String eventType) {
        switch (unknownTypeBehavior) {
            case SKIP:
                LOG.warn("Skipping unknown Salesforce event: {}", eventType);
                return Collections.emptyList();
            case FAIL:
                throw new PayloadRoutingException("Unknown Salesforce event format: " + eventType);
            case DEFAULT_TOPIC:
            default:
                LOG.info("Routing unknown Salesforce event '{}' to default topic: {}", eventType, defaultTopic);
                return Collections.singletonList(createRecord(defaultTopic, payload, eventType, Collections.emptyMap()));
        }
    }

    private RoutedRecord createRecord(String topicSuffix, Map<String, Object> data, String eventType,
                                      Map<String, Object> keyFields) {
        String fullTopic = topicPrefix + topicSuffix;
        String json;
        try {
            json = objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new PayloadRoutingException("Failed to serialize record payload to JSON", e);
        }
        return new RoutedRecord(fullTopic, json, eventType, keyFields);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMapField(Map<String, Object> map, String field) {
        Object value = map.get(field);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }
}