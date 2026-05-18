package org.apache.camel.kafkaconnector.nettyhttp.routing.stripe;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StripePayloadStrategy implements PayloadRoutingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(StripePayloadStrategy.class);

    static final String HEADER_SIGNATURE = "Stripe-Signature";
    private static final String DELETED_FIELD = "__deleted";
    private static final String CHANGE_TYPE_FIELD = "__changeType";

    // Fan-out candidates per resource type
    // Stripe nests list objects as {object: "list", data: [...]}
    private static final Map<String, List<String>> FANOUT_CANDIDATES = new LinkedHashMap<>();
    static {
        FANOUT_CANDIDATES.put("invoice", Arrays.asList("lines"));
        FANOUT_CANDIDATES.put("charge", Arrays.asList("refunds"));
        FANOUT_CANDIDATES.put("subscription", Arrays.asList("items"));
    }

    private String topicPrefix = "";
    private UnknownTypeBehavior unknownTypeBehavior = UnknownTypeBehavior.DEFAULT_TOPIC;
    private String defaultTopic = "unknown";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Set<String> fanoutFields = Collections.emptySet();
    private boolean includeEvent = true;
    private Set<String> allowedObjects = null;

    private String signingSecret = null;
    private long timestampToleranceSeconds = 300; // 5 minutes default

    @Override
    public void configure(String topicPrefix, UnknownTypeBehavior unknownTypeBehavior, String defaultTopic) {
        this.topicPrefix = topicPrefix != null ? topicPrefix : "";
        this.unknownTypeBehavior = unknownTypeBehavior != null ? unknownTypeBehavior : UnknownTypeBehavior.DEFAULT_TOPIC;
        this.defaultTopic = defaultTopic != null ? defaultTopic : "unknown";
    }

    @Override
    public void configureAdvanced(Set<String> fanoutFields, boolean flattenDetail,
                                  String flattenDetailPrefix, boolean includeEvent,
                                  Set<String> allowedObjects) {
        this.fanoutFields = fanoutFields != null ? fanoutFields : Collections.emptySet();
        this.includeEvent = includeEvent;
        this.allowedObjects = (allowedObjects != null && !allowedObjects.isEmpty()) ? allowedObjects : null;
    }

    @Override
    public void configureHmac(String signingSecret) {
        this.signingSecret = signingSecret;
    }

    public void setTimestampToleranceSeconds(long seconds) {
        this.timestampToleranceSeconds = seconds;
    }

    @Override
    public List<RoutedRecord> route(Map<String, Object> payload) {
        return route(payload, Collections.emptyMap());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(Map<String, Object> payload, Map<String, Object> headers) {
        // Signature verification
        if (signingSecret != null && !signingSecret.isEmpty()) {
            verifySignature(headers);
        }

        // Extract event type from body
        String eventType = (String) payload.get("type");
        if (eventType == null || eventType.isEmpty()) {
            return handleUnknownType(payload, "<missing-type-field>");
        }

        // Parse resource and action: "customer.created" → resource="customer", action="created"
        // "customer.subscription.updated" → resource="customer", action="subscription.updated"
        int firstDot = eventType.indexOf('.');
        String resource;
        String action;
        if (firstDot >= 0) {
            resource = eventType.substring(0, firstDot);
            action = eventType.substring(firstDot + 1);
        } else {
            resource = eventType;
            action = "unknown";
        }

        // Allowed objects filter
        if (allowedObjects != null && !allowedObjects.contains(resource)) {
            return handleUnknownType(payload, eventType);
        }

        // Extract the last action segment for change type mapping
        String lastAction = action.contains(".") ? action.substring(action.lastIndexOf('.') + 1) : action;
        String changeType = mapActionToChangeType(lastAction);
        boolean isDeleted = "deleted".equals(lastAction);
        String op = changeTypeToOp(changeType);

        // Extract data.object as the main payload
        Map<String, Object> data = getMapField(payload, "data");
        Map<String, Object> resourceObject = data != null ? getMapField(data, "object") : null;

        if (resourceObject == null) {
            return handleUnknownType(payload, eventType);
        }

        // Build output payload from data.object + synthetic fields
        Map<String, Object> outputPayload = new LinkedHashMap<>(resourceObject);
        outputPayload.put(CHANGE_TYPE_FIELD, changeType);
        outputPayload.put(DELETED_FIELD, isDeleted);

        if (includeEvent) {
            outputPayload.put("_event_id", payload.get("id"));
            outputPayload.put("_event_type", eventType);
            outputPayload.put("_event_created", payload.get("created"));
            outputPayload.put("_api_version", payload.get("api_version"));
            outputPayload.put("_livemode", payload.get("livemode"));

            // Include previous_attributes for *.updated events
            Map<String, Object> previousAttributes = data != null ? getMapField(data, "previous_attributes") : null;
            if (previousAttributes != null && !previousAttributes.isEmpty()) {
                outputPayload.put("_previous_attributes", previousAttributes);
            }
        }

        // Extract ID for key
        Object resourceId = resourceObject.get("id");
        Map<String, Object> key = resourceId != null
                ? keyOf("id", resourceId)
                : Collections.emptyMap();

        List<RoutedRecord> records = new ArrayList<>();
        records.add(createRecord(resource, outputPayload, eventType, key, op));

        // Fan-out nested list objects
        for (String fieldName : getFanoutFieldsForResource(resource)) {
            Object nested = resourceObject.get(fieldName);
            // Stripe nests lists as {object: "list", data: [...]}
            List<Map<String, Object>> items = extractListData(nested);
            if (items == null) continue;

            String fanoutTopic = resource + "_" + fieldName;
            for (Map<String, Object> item : items) {
                Map<String, Object> itemRecord = new LinkedHashMap<>(item);
                Object itemId = item.get("id");
                Map<String, Object> fanoutKey;
                if (itemId != null && resourceId != null) {
                    fanoutKey = keyOf("id", resourceId, "item_id", itemId);
                } else if (resourceId != null) {
                    fanoutKey = keyOf("id", resourceId);
                } else {
                    fanoutKey = Collections.emptyMap();
                }
                records.add(createRecord(fanoutTopic, itemRecord, eventType, fanoutKey, op));
            }
        }

        return records;
    }

    // --- Change Type Mapping ---

    static String mapActionToChangeType(String action) {
        if (action == null) return "UNKNOWN";
        switch (action.toLowerCase()) {
            case "created":
                return "CREATE";
            case "updated":
                return "UPDATE";
            case "deleted":
                return "DELETE";
            case "succeeded":
                return "SUCCEEDED";
            case "failed":
            case "payment_failed":
                return "FAILED";
            case "canceled":
                return "CANCELED";
            case "refunded":
                return "REFUNDED";
            case "captured":
                return "CAPTURED";
            case "expired":
                return "EXPIRED";
            case "completed":
                return "COMPLETED";
            case "paid":
                return "PAID";
            case "finalized":
                return "FINALIZED";
            case "voided":
                return "VOIDED";
            default:
                return action.toUpperCase();
        }
    }

    static String changeTypeToOp(String changeType) {
        if (changeType == null) return "u";
        switch (changeType) {
            case "CREATE":
                return "c";
            case "DELETE":
                return "d";
            default:
                return "u";
        }
    }

    // --- Signature Verification ---

    private void verifySignature(Map<String, Object> headers) {
        String signatureHeader = getHeaderString(headers, HEADER_SIGNATURE);
        if (signatureHeader == null) {
            throw new PayloadRoutingException(
                    "Stripe signature verification enabled but Stripe-Signature header missing");
        }

        Object rawBodyObj = headers.get("__rawBody");
        if (rawBodyObj == null) {
            throw new PayloadRoutingException(
                    "Stripe signature verification requires raw body but __rawBody not found");
        }
        String rawBody = String.valueOf(rawBodyObj);

        // Parse Stripe-Signature: t=timestamp,v1=signature[,v1=signature2]
        String timestamp = null;
        List<String> signatures = new ArrayList<>();
        for (String part : signatureHeader.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2) {
                if ("t".equals(kv[0])) {
                    timestamp = kv[1];
                } else if ("v1".equals(kv[0])) {
                    signatures.add(kv[1]);
                }
            }
        }

        if (timestamp == null || signatures.isEmpty()) {
            throw new PayloadRoutingException("Stripe-Signature header malformed: " + signatureHeader);
        }

        // Check timestamp tolerance
        if (timestampToleranceSeconds > 0) {
            long eventTime = Long.parseLong(timestamp);
            long now = System.currentTimeMillis() / 1000;
            if (Math.abs(now - eventTime) > timestampToleranceSeconds) {
                throw new PayloadRoutingException(
                        "Stripe webhook timestamp too old (tolerance: " + timestampToleranceSeconds + "s)");
            }
        }

        // Compute expected signature: HMAC-SHA256 of "{timestamp}.{rawBody}"
        String signedPayload = timestamp + "." + rawBody;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(
                    signingSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKey);
            byte[] hash = mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8));
            String computedSignature = bytesToHex(hash);

            // Check against all v1 signatures (Stripe sends multiple during secret rotation)
            boolean matched = false;
            for (String sig : signatures) {
                if (computedSignature.equals(sig)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw new PayloadRoutingException("Stripe signature verification failed");
            }
            LOG.debug("Stripe signature verification passed");
        } catch (PayloadRoutingException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadRoutingException("Stripe signature verification error", e);
        }
    }

    // --- Fan-Out ---

    private List<String> getFanoutFieldsForResource(String resource) {
        List<String> candidates = FANOUT_CANDIDATES.getOrDefault(resource, Collections.emptyList());
        List<String> result = new ArrayList<>();
        for (String candidate : candidates) {
            if (fanoutFields.contains(resource + "." + candidate)) {
                result.add(candidate);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractListData(Object nested) {
        if (nested instanceof Map) {
            Map<String, Object> listObj = (Map<String, Object>) nested;
            if ("list".equals(listObj.get("object"))) {
                Object data = listObj.get("data");
                if (data instanceof List) {
                    return (List<Map<String, Object>>) data;
                }
            }
        }
        if (nested instanceof List) {
            return (List<Map<String, Object>>) nested;
        }
        return null;
    }

    // --- Helpers ---

    private List<RoutedRecord> handleUnknownType(Map<String, Object> payload, String eventType) {
        switch (unknownTypeBehavior) {
            case SKIP:
                LOG.warn("Skipping unknown Stripe event: {}", eventType);
                return Collections.emptyList();
            case FAIL:
                throw new PayloadRoutingException("Unknown Stripe event: " + eventType);
            case DEFAULT_TOPIC:
            default:
                LOG.info("Routing unknown Stripe event '{}' to default topic: {}", eventType, defaultTopic);
                return Collections.singletonList(createRecord(defaultTopic, payload, eventType, Collections.emptyMap()));
        }
    }

    private RoutedRecord createRecord(String topicSuffix, Map<String, Object> data,
                                      String eventType, Map<String, Object> keyFields) {
        return createRecord(topicSuffix, data, eventType, keyFields, null);
    }

    private RoutedRecord createRecord(String topicSuffix, Map<String, Object> data,
                                      String eventType, Map<String, Object> keyFields, String op) {
        String fullTopic = topicPrefix + topicSuffix;
        String json;
        try {
            json = objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new PayloadRoutingException("Failed to serialize record payload to JSON", e);
        }
        return new RoutedRecord(fullTopic, json, eventType, keyFields, op);
    }

    private static Map<String, Object> keyOf(String field, Object value) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(field, value);
        return key;
    }

    private static Map<String, Object> keyOf(String f1, Object v1, String f2, Object v2) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(f1, v1);
        key.put(f2, v2);
        return key;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMapField(Map<String, Object> map, String field) {
        Object value = map.get(field);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    private static String getHeaderString(Map<String, Object> headers, String key) {
        Object value = headers.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
