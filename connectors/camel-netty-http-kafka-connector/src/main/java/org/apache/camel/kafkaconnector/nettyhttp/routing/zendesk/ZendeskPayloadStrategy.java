package org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZendeskPayloadStrategy implements PayloadRoutingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ZendeskPayloadStrategy.class);

    private static final String TYPE_FIELD = "type";
    private static final String DETAIL_FIELD = "detail";
    private static final String EVENT_FIELD = "event";
    private static final String CONTEXT_PREFIX = "_ctx_";

    private String topicPrefix = "";
    private UnknownTypeBehavior unknownTypeBehavior = UnknownTypeBehavior.DEFAULT_TOPIC;
    private String defaultTopic = "unknown";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(String topicPrefix, UnknownTypeBehavior unknownTypeBehavior, String defaultTopic) {
        this.topicPrefix = topicPrefix != null ? topicPrefix : "";
        this.unknownTypeBehavior = unknownTypeBehavior != null ? unknownTypeBehavior : UnknownTypeBehavior.DEFAULT_TOPIC;
        this.defaultTopic = defaultTopic != null ? defaultTopic : "unknown";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(Map<String, Object> payload) {
        String eventType = getStringField(payload, TYPE_FIELD);

        if (eventType == null || eventType.isEmpty()) {
            return handleUnknownType(payload, "<missing>");
        }

        // Parse Zendesk event type: "zen:event-type:DOMAIN.EVENT_NAME"
        String domain = extractDomain(eventType);
        if (domain == null) {
            return handleUnknownType(payload, eventType);
        }

        switch (domain) {
            case "ticket":
                return routeTicketEvent(payload, eventType);
            case "user":
                return routeUserEvent(payload, eventType);
            case "organization":
                return routeOrganizationEvent(payload, eventType);
            default:
                return handleUnknownType(payload, eventType);
        }
    }

    // --- Ticket Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routeTicketEvent(Map<String, Object> payload, String eventType) {
        List<RoutedRecord> records = new ArrayList<>();
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk ticket event missing required field: detail.id");
        }

        // Main ticket event record (full payload)
        records.add(createRecord("ticket_events", payload, eventType));

        Object ticketId = detail.get("id");

        // Split tags array
        Object tags = detail.get("tags");
        if (tags instanceof List) {
            for (Object tag : (List<?>) tags) {
                Map<String, Object> tagRecord = new LinkedHashMap<>();
                if (tag instanceof Map) {
                    tagRecord.putAll((Map<String, Object>) tag);
                } else {
                    tagRecord.put("value", tag);
                }
                tagRecord.put(CONTEXT_PREFIX + "ticket_id", ticketId);
                records.add(createRecord("ticket_tags", tagRecord, eventType));
            }
        }

        // Split custom_fields array
        Object customFields = detail.get("custom_fields");
        if (customFields instanceof List) {
            for (Object field : (List<?>) customFields) {
                if (field instanceof Map) {
                    Map<String, Object> fieldRecord = new LinkedHashMap<>((Map<String, Object>) field);
                    fieldRecord.put(CONTEXT_PREFIX + "ticket_id", ticketId);
                    records.add(createRecord("ticket_custom_fields", fieldRecord, eventType));
                }
            }
        }

        // Handle comment_added events - extract comment from event object
        String eventName = extractEventName(eventType);
        if ("comment_added".equals(eventName)) {
            Map<String, Object> event = getMapField(payload, EVENT_FIELD);
            if (event != null) {
                Object comment = event.get("comment");
                if (comment instanceof Map) {
                    Map<String, Object> commentRecord = new LinkedHashMap<>((Map<String, Object>) comment);
                    commentRecord.put(CONTEXT_PREFIX + "ticket_id", ticketId);
                    if (detail.get("subject") != null) {
                        commentRecord.put(CONTEXT_PREFIX + "ticket_subject", detail.get("subject"));
                    }
                    records.add(createRecord("ticket_comments", commentRecord, eventType));
                }
            }
        }

        // Split collaborators array
        Object collaborators = detail.get("collaborators");
        if (collaborators instanceof List) {
            for (Object collab : (List<?>) collaborators) {
                if (collab instanceof Map) {
                    Map<String, Object> collabRecord = new LinkedHashMap<>((Map<String, Object>) collab);
                    collabRecord.put(CONTEXT_PREFIX + "ticket_id", ticketId);
                    records.add(createRecord("ticket_collaborators", collabRecord, eventType));
                }
            }
        }

        // Split followers array
        Object followers = detail.get("followers");
        if (followers instanceof List) {
            for (Object follower : (List<?>) followers) {
                if (follower instanceof Map) {
                    Map<String, Object> followerRecord = new LinkedHashMap<>((Map<String, Object>) follower);
                    followerRecord.put(CONTEXT_PREFIX + "ticket_id", ticketId);
                    records.add(createRecord("ticket_followers", followerRecord, eventType));
                }
            }
        }

        return records;
    }

    // --- User Events ---

    private List<RoutedRecord> routeUserEvent(Map<String, Object> payload, String eventType) {
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk user event missing required field: detail.id");
        }

        return Collections.singletonList(createRecord("user_events", payload, eventType));
    }

    // --- Organization Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routeOrganizationEvent(Map<String, Object> payload, String eventType) {
        List<RoutedRecord> records = new ArrayList<>();
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk organization event missing required field: detail.id");
        }

        // Main organization event record
        records.add(createRecord("organization_events", payload, eventType));

        Object orgId = detail.get("id");

        // Split tags array
        Object tags = detail.get("tags");
        if (tags instanceof List) {
            for (Object tag : (List<?>) tags) {
                Map<String, Object> tagRecord = new LinkedHashMap<>();
                if (tag instanceof Map) {
                    tagRecord.putAll((Map<String, Object>) tag);
                } else {
                    tagRecord.put("value", tag);
                }
                tagRecord.put(CONTEXT_PREFIX + "organization_id", orgId);
                records.add(createRecord("organization_tags", tagRecord, eventType));
            }
        }

        return records;
    }

    // --- Helpers ---

    private List<RoutedRecord> handleUnknownType(Map<String, Object> payload, String eventType) {
        switch (unknownTypeBehavior) {
            case SKIP:
                LOG.warn("Skipping unknown Zendesk event type: {}", eventType);
                return Collections.emptyList();
            case FAIL:
                throw new PayloadRoutingException("Unknown Zendesk event type: " + eventType);
            case DEFAULT_TOPIC:
            default:
                LOG.info("Routing unknown Zendesk event type '{}' to default topic: {}", eventType, defaultTopic);
                return Collections.singletonList(createRecord(defaultTopic, payload, eventType));
        }
    }

    /**
     * Extract domain from Zendesk event type.
     * Format: "zen:event-type:DOMAIN.EVENT_NAME"
     * Also supports simplified format: "DOMAIN.EVENT_NAME"
     */
    private String extractDomain(String eventType) {
        if (eventType.startsWith("zen:event-type:")) {
            String remainder = eventType.substring("zen:event-type:".length());
            int dotIndex = remainder.indexOf('.');
            if (dotIndex > 0) {
                return remainder.substring(0, dotIndex);
            }
            return remainder;
        }
        int dotIndex = eventType.indexOf('.');
        if (dotIndex > 0) {
            return eventType.substring(0, dotIndex);
        }
        return null;
    }

    /**
     * Extract event name from Zendesk event type.
     * Format: "zen:event-type:DOMAIN.EVENT_NAME"
     */
    private String extractEventName(String eventType) {
        int lastDot = eventType.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < eventType.length() - 1) {
            return eventType.substring(lastDot + 1);
        }
        return null;
    }

    private RoutedRecord createRecord(String topicSuffix, Map<String, Object> data, String eventType) {
        String fullTopic = topicPrefix + topicSuffix;
        String json;
        try {
            json = objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new PayloadRoutingException("Failed to serialize record payload to JSON", e);
        }
        return new RoutedRecord(fullTopic, json, eventType);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMapField(Map<String, Object> map, String field) {
        Object value = map.get(field);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    private static String getStringField(Map<String, Object> map, String field) {
        Object value = map.get(field);
        return value != null ? String.valueOf(value) : null;
    }
}
