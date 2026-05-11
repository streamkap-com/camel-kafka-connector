package org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk;

import java.util.ArrayList;
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

public class ZendeskPayloadStrategy implements PayloadRoutingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ZendeskPayloadStrategy.class);

    private static final String TYPE_FIELD = "type";
    private static final String DETAIL_FIELD = "detail";
    private static final String EVENT_FIELD = "event";
    private static final String CONTEXT_PREFIX = "_ctx_";
    private static final String DELETED_FIELD = "__deleted";

    private String topicPrefix = "";
    private UnknownTypeBehavior unknownTypeBehavior = UnknownTypeBehavior.DEFAULT_TOPIC;
    private String defaultTopic = "unknown";
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Advanced config
    private Set<String> fanoutFields = Collections.emptySet();
    private boolean flattenDetail = false;
    private String flattenDetailPrefix = "detail_";
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
        this.fanoutFields = fanoutFields != null ? fanoutFields : Collections.emptySet();
        this.flattenDetail = flattenDetail;
        this.flattenDetailPrefix = flattenDetailPrefix != null ? flattenDetailPrefix : "detail_";
        this.includeEvent = includeEvent;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(Map<String, Object> payload) {
        String eventType = getStringField(payload, TYPE_FIELD);

        if (eventType == null || eventType.isEmpty()) {
            return handleUnknownType(payload, "<missing>");
        }

        String domain = extractDomain(eventType);
        if (domain == null) {
            return handleUnknownType(payload, eventType);
        }

        switch (domain) {
            case "ticket":
                return routeTicketEvent(payload, eventType, domain);
            case "user":
                return routeUserEvent(payload, eventType, domain);
            case "organization":
                return routeOrganizationEvent(payload, eventType, domain);
            case "article":
                return routeDetailIdEvent(payload, eventType, domain, "article_events");
            case "community_post":
                return routeDetailIdEvent(payload, eventType, domain, "community_post_events");
            case "messaging_ticket":
                return routeDetailIdEvent(payload, eventType, domain, "messaging_events");
            case "agent":
                return routeAgentEvent(payload, eventType, domain);
            case "omnichannel_config":
                return routeAccountIdEvent(payload, eventType, "omnichannel_config_events");
            case "messaging_live_metrics":
                return routeAccountIdEvent(payload, eventType, "messaging_metrics_events");
            default:
                return handleUnknownType(payload, eventType);
        }
    }

    // --- Ticket Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routeTicketEvent(Map<String, Object> payload, String eventType, String domain) {
        List<RoutedRecord> records = new ArrayList<>();
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk ticket event missing required field: detail.id");
        }

        Object ticketId = detail.get("id");
        Object eventId = payload.get("id");

        // Main ticket event — key field matches flattened value field name
        String idKeyField = flattenDetailPrefix + "id";
        Map<String, Object> mainPayload = buildMainPayload(payload, domain, eventType);
        records.add(createRecord("ticket_events", mainPayload, eventType, keyOf(idKeyField, ticketId)));

        // Fan-out: tags — key: {ticket_id, value}
        if (shouldFanout(domain, "tags")) {
            Object tags = detail.get("tags");
            if (tags instanceof List) {
                for (Object tag : (List<?>) tags) {
                    Map<String, Object> tagRecord = new LinkedHashMap<>();
                    Object tagValue;
                    if (tag instanceof Map) {
                        tagRecord.putAll((Map<String, Object>) tag);
                        tagValue = ((Map<?, ?>) tag).get("value");
                    } else {
                        tagRecord.put("value", tag);
                        tagValue = tag;
                    }
                    addFanoutContext(tagRecord, eventId, idKeyField, ticketId);
                    records.add(createRecord("ticket_tags", tagRecord, eventType,
                            keyOf(idKeyField, ticketId, "value", tagValue)));
                }
            }
        }

        // Fan-out: custom_fields — key: {ticket_id, id}
        if (shouldFanout(domain, "custom_fields")) {
            Object customFields = detail.get("custom_fields");
            if (customFields instanceof List) {
                for (Object field : (List<?>) customFields) {
                    if (field instanceof Map) {
                        Map<String, Object> fieldRecord = new LinkedHashMap<>((Map<String, Object>) field);
                        addFanoutContext(fieldRecord, eventId, idKeyField, ticketId);
                        Object fieldId = ((Map<?, ?>) field).get("id");
                        records.add(createRecord("ticket_custom_fields", fieldRecord, eventType,
                                keyOf(idKeyField, ticketId, "id", fieldId)));
                    }
                }
            }
        }

        // Fan-out: comments — key: {id} (comment's own ID)
        if (shouldFanout(domain, "comments")) {
            String eventName = extractEventName(eventType);
            if ("comment_added".equals(eventName)) {
                Map<String, Object> event = getMapField(payload, EVENT_FIELD);
                if (event != null) {
                    Object comment = event.get("comment");
                    if (comment instanceof Map) {
                        Map<String, Object> commentRecord = new LinkedHashMap<>((Map<String, Object>) comment);
                        addFanoutContext(commentRecord, eventId, idKeyField, ticketId);
                        if (detail.get("subject") != null) {
                            commentRecord.put(CONTEXT_PREFIX + "ticket_subject", detail.get("subject"));
                        }
                        Object commentId = ((Map<?, ?>) comment).get("id");
                        records.add(createRecord("ticket_comments", commentRecord, eventType,
                                keyOf("id", commentId)));
                    }
                }
            }
        }

        // Fan-out: collaborators — key: {ticket_id, id}
        if (shouldFanout(domain, "collaborators")) {
            Object collaborators = detail.get("collaborators");
            if (collaborators instanceof List) {
                for (Object collab : (List<?>) collaborators) {
                    if (collab instanceof Map) {
                        Map<String, Object> collabRecord = new LinkedHashMap<>((Map<String, Object>) collab);
                        addFanoutContext(collabRecord, eventId, idKeyField, ticketId);
                        Object collabId = ((Map<?, ?>) collab).get("id");
                        records.add(createRecord("ticket_collaborators", collabRecord, eventType,
                                keyOf(idKeyField, ticketId, "id", collabId)));
                    }
                }
            }
        }

        // Fan-out: followers — key: {ticket_id, id}
        if (shouldFanout(domain, "followers")) {
            Object followers = detail.get("followers");
            if (followers instanceof List) {
                for (Object follower : (List<?>) followers) {
                    if (follower instanceof Map) {
                        Map<String, Object> followerRecord = new LinkedHashMap<>((Map<String, Object>) follower);
                        addFanoutContext(followerRecord, eventId, idKeyField, ticketId);
                        Object followerId = ((Map<?, ?>) follower).get("id");
                        records.add(createRecord("ticket_followers", followerRecord, eventType,
                                keyOf(idKeyField, ticketId, "id", followerId)));
                    }
                }
            }
        }

        return records;
    }

    // --- User Events ---

    private List<RoutedRecord> routeUserEvent(Map<String, Object> payload, String eventType, String domain) {
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk user event missing required field: detail.id");
        }

        String idKeyField = flattenDetailPrefix + "id";
        Map<String, Object> mainPayload = buildMainPayload(payload, domain, eventType);
        return Collections.singletonList(createRecord("user_events", mainPayload, eventType,
                keyOf(idKeyField, detail.get("id"))));
    }

    // --- Organization Events ---

    @SuppressWarnings("unchecked")
    private List<RoutedRecord> routeOrganizationEvent(Map<String, Object> payload, String eventType, String domain) {
        List<RoutedRecord> records = new ArrayList<>();
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk organization event missing required field: detail.id");
        }

        Object orgId = detail.get("id");
        Object eventId = payload.get("id");
        String idKeyField = flattenDetailPrefix + "id";

        Map<String, Object> mainPayload = buildMainPayload(payload, domain, eventType);
        records.add(createRecord("organization_events", mainPayload, eventType,
                keyOf(idKeyField, orgId)));

        // Fan-out: tags — key: {detail_id, value}
        if (shouldFanout(domain, "tags")) {
            Object tags = detail.get("tags");
            if (tags instanceof List) {
                for (Object tag : (List<?>) tags) {
                    Map<String, Object> tagRecord = new LinkedHashMap<>();
                    Object tagValue;
                    if (tag instanceof Map) {
                        tagRecord.putAll((Map<String, Object>) tag);
                        tagValue = ((Map<?, ?>) tag).get("value");
                    } else {
                        tagRecord.put("value", tag);
                        tagValue = tag;
                    }
                    addFanoutContext(tagRecord, eventId, idKeyField, orgId);
                    records.add(createRecord("organization_tags", tagRecord, eventType,
                            keyOf(idKeyField, orgId, "value", tagValue)));
                }
            }
        }

        return records;
    }

    // --- Generic: Events with detail.id ---

    private List<RoutedRecord> routeDetailIdEvent(Map<String, Object> payload, String eventType,
                                                   String domain, String topicSuffix) {
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null || detail.get("id") == null) {
            throw new PayloadRoutingException("Zendesk " + domain + " event missing required field: detail.id");
        }

        String idKeyField = flattenDetailPrefix + "id";
        Map<String, Object> mainPayload = buildMainPayload(payload, domain, eventType);
        return Collections.singletonList(createRecord(topicSuffix, mainPayload, eventType,
                keyOf(idKeyField, detail.get("id"))));
    }

    // --- Agent Availability Events (keyed by agent_id) ---

    private List<RoutedRecord> routeAgentEvent(Map<String, Object> payload, String eventType, String domain) {
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);

        if (detail == null) {
            throw new PayloadRoutingException("Zendesk agent event missing required field: detail");
        }

        // Agent events use agent_id as key (not id)
        Object agentId = detail.get("agent_id");
        String agentIdField = flattenDetailPrefix + "agent_id";
        if (agentId == null) {
            agentId = detail.get("id");
            agentIdField = flattenDetailPrefix + "id";
        }
        if (agentId == null) {
            throw new PayloadRoutingException("Zendesk agent event missing required field: detail.agent_id or detail.id");
        }

        Map<String, Object> mainPayload = buildMainPayload(payload, domain, eventType);
        return Collections.singletonList(createRecord("agent_events", mainPayload, eventType,
                keyOf(agentIdField, agentId)));
    }

    // --- Account-level Events (keyed by account_id) ---

    private List<RoutedRecord> routeAccountIdEvent(Map<String, Object> payload, String eventType,
                                                    String topicSuffix) {
        // Account-level events may have account_id at top level or in detail
        Object accountId = payload.get("account_id");
        Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);
        if (accountId == null && detail != null) {
            accountId = detail.get("account_id");
        }

        Map<String, Object> key = accountId != null
                ? keyOf("account_id", accountId)
                : Collections.emptyMap();

        return Collections.singletonList(createRecord(topicSuffix, payload, eventType, key));
    }

    // --- Payload Transformation ---

    @SuppressWarnings("unchecked")
    private Map<String, Object> buildMainPayload(Map<String, Object> payload, String domain, String eventType) {
        boolean isDeleted = isDeleteEvent(eventType);

        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String key = entry.getKey();
            if (flattenDetail && DETAIL_FIELD.equals(key)) {
                continue;
            }
            if (!includeEvent && EVENT_FIELD.equals(key)) {
                continue;
            }
            result.put(key, entry.getValue());
        }

        if (flattenDetail) {
            Map<String, Object> detail = getMapField(payload, DETAIL_FIELD);
            if (detail != null) {
                for (Map.Entry<String, Object> entry : detail.entrySet()) {
                    result.put(flattenDetailPrefix + entry.getKey(), entry.getValue());
                }
            }
        }

        // Add __deleted field based on event type
        result.put(DELETED_FIELD, isDeleted);

        return result;
    }

    private boolean isDeleteEvent(String eventType) {
        if (eventType == null) {
            return false;
        }
        String eventName = extractEventName(eventType);
        if (eventName == null) {
            return false;
        }
        // Exclude: soft_deleted (recoverable), undeleted (restore)
        if ("soft_deleted".equals(eventName) || "undeleted".equals(eventName)) {
            return false;
        }
        // Match any event ending with "deleted" or "removed":
        // deleted, permanently_deleted, channel_deleted, group_membership_deleted,
        // removed, vote_removed, work_item_removed, etc.
        return eventName.endsWith("deleted") || eventName.endsWith("removed");
    }

    // --- Fanout Context ---

    private void addFanoutContext(Map<String, Object> record, Object eventId, String parentIdField, Object parentId) {
        // Add parent ID matching key field name (for sink connector upsert)
        record.put(parentIdField, parentId);
        // Add event ID for correlation (always with _ctx_ prefix since it's context-only)
        if (eventId != null) {
            record.put(CONTEXT_PREFIX + "event_id", eventId);
        }
    }

    // --- Key Helpers ---

    private Map<String, Object> keyOf(String field, Object value) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(field, value);
        return key;
    }

    private Map<String, Object> keyOf(String f1, Object v1, String f2, Object v2) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(f1, v1);
        key.put(f2, v2);
        return key;
    }

    // --- Fanout Control ---

    private boolean shouldFanout(String domain, String field) {
        return fanoutFields.contains(domain + "." + field);
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
                return Collections.singletonList(createRecord(defaultTopic, payload, eventType, Collections.emptyMap()));
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

    private String extractEventName(String eventType) {
        int lastDot = eventType.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < eventType.length() - 1) {
            return eventType.substring(lastDot + 1);
        }
        return null;
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

    private static String getStringField(Map<String, Object> map, String field) {
        Object value = map.get(field);
        return value != null ? String.valueOf(value) : null;
    }
}