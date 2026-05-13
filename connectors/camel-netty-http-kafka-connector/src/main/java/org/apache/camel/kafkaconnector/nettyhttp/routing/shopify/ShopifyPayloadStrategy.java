package org.apache.camel.kafkaconnector.nettyhttp.routing.shopify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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

public class ShopifyPayloadStrategy implements PayloadRoutingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ShopifyPayloadStrategy.class);

    // Shopify HTTP header keys
    static final String HEADER_TOPIC = "X-Shopify-Topic";
    static final String HEADER_SHOP_DOMAIN = "X-Shopify-Shop-Domain";
    static final String HEADER_HMAC = "X-Shopify-Hmac-Sha256";
    static final String HEADER_API_VERSION = "X-Shopify-API-Version";
    static final String HEADER_WEBHOOK_ID = "X-Shopify-Webhook-Id";
    static final String HEADER_EVENT_ID = "X-Shopify-Event-Id";
    static final String HEADER_TRIGGERED_AT = "X-Shopify-Triggered-At";

    // Synthetic fields
    private static final String DELETED_FIELD = "__deleted";
    private static final String CHANGE_TYPE_FIELD = "__changeType";
    private static final String CONTEXT_PREFIX = "_ctx_";

    // Fan-out candidates per resource type
    private static final Map<String, List<String>> FANOUT_CANDIDATES = new LinkedHashMap<>();
    static {
        FANOUT_CANDIDATES.put("orders", Arrays.asList(
                "line_items", "shipping_lines", "discount_codes", "tax_lines", "fulfillments"));
        FANOUT_CANDIDATES.put("products", Arrays.asList("variants", "images", "options"));
        FANOUT_CANDIDATES.put("customers", Arrays.asList("addresses"));
        FANOUT_CANDIDATES.put("draft_orders", Arrays.asList("line_items"));
        FANOUT_CANDIDATES.put("fulfillments", Arrays.asList("line_items"));
    }

    private String topicPrefix = "";
    private UnknownTypeBehavior unknownTypeBehavior = UnknownTypeBehavior.DEFAULT_TOPIC;
    private String defaultTopic = "unknown";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Set<String> fanoutFields = Collections.emptySet();
    private boolean includeEvent = true;
    private Set<String> allowedObjects = null;

    private String hmacSecret = null;

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
    public void configureHmac(String hmacSecret) {
        this.hmacSecret = hmacSecret;
    }

    @Override
    public List<RoutedRecord> route(Map<String, Object> payload) {
        LOG.warn("Shopify routing requires HTTP headers (X-Shopify-Topic). Routing to default topic.");
        return handleUnknownType(payload, "<no-headers>");
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(Map<String, Object> payload, Map<String, Object> headers) {
        // HMAC verification
        if (hmacSecret != null && !hmacSecret.isEmpty()) {
            verifyHmac(headers);
        }

        // Extract topic from header
        String shopifyTopic = getHeaderString(headers, HEADER_TOPIC);
        if (shopifyTopic == null || shopifyTopic.isEmpty()) {
            return handleUnknownType(payload, "<missing-topic-header>");
        }

        // Parse resource and action
        String resource;
        String action;
        if (shopifyTopic.contains("/")) {
            String[] parts = shopifyTopic.split("/", 2);
            resource = parts[0];
            action = parts.length > 1 ? parts[1] : "unknown";
        } else if (shopifyTopic.contains(".")) {
            int dotIndex = shopifyTopic.indexOf('.');
            resource = shopifyTopic.substring(0, dotIndex);
            action = shopifyTopic.substring(dotIndex + 1);
        } else {
            resource = shopifyTopic;
            action = "unknown";
        }

        // Allowed objects filter
        if (allowedObjects != null && !allowedObjects.contains(resource)) {
            return handleUnknownType(payload, shopifyTopic);
        }

        String changeType = mapActionToChangeType(action);
        boolean isDeleted = isDeleteAction(action);
        Object resourceId = payload.get("id");
        String eventType = resource + "." + action;

        // Build main output payload
        Map<String, Object> outputPayload = buildMainPayload(payload, headers, changeType, isDeleted);

        List<RoutedRecord> records = new ArrayList<>();
        Map<String, Object> key = resourceId != null
                ? keyOf("id", resourceId)
                : Collections.emptyMap();
        String op = changeTypeToOp(changeType);
        records.add(createRecord(resource, outputPayload, eventType, key, op));

        // Fan-out nested arrays
        for (String fieldName : getFanoutFieldsForResource(resource)) {
            Object nested = payload.get(fieldName);
            if (nested instanceof List) {
                List<?> items = (List<?>) nested;
                for (Object item : items) {
                    if (item instanceof Map) {
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        Map<String, Object> itemRecord = new LinkedHashMap<>(itemMap);
                        addFanoutContext(itemRecord, headers, resourceId);

                        Object itemId = itemMap.get("id");
                        String fanoutTopic = resource + "_" + fieldName;
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
            }
        }

        return records;
    }

    private Map<String, Object> buildMainPayload(Map<String, Object> payload,
                                                   Map<String, Object> headers,
                                                   String changeType, boolean isDeleted) {
        Map<String, Object> result = new LinkedHashMap<>(payload);

        result.put(CHANGE_TYPE_FIELD, changeType);
        result.put(DELETED_FIELD, isDeleted);

        if (includeEvent) {
            putIfHeaderPresent(result, "_shop_domain", headers, HEADER_SHOP_DOMAIN);
            putIfHeaderPresent(result, "_event_id", headers, HEADER_EVENT_ID);
            putIfHeaderPresent(result, "_triggered_at", headers, HEADER_TRIGGERED_AT);
            putIfHeaderPresent(result, "_api_version", headers, HEADER_API_VERSION);
            putIfHeaderPresent(result, "_webhook_id", headers, HEADER_WEBHOOK_ID);
        }

        return result;
    }

    private void addFanoutContext(Map<String, Object> record, Map<String, Object> headers,
                                  Object parentId) {
        if (parentId != null) {
            record.put("id", parentId);
        }
        String eventId = getHeaderString(headers, HEADER_EVENT_ID);
        if (eventId != null) {
            record.put(CONTEXT_PREFIX + "event_id", eventId);
        }
        String shopDomain = getHeaderString(headers, HEADER_SHOP_DOMAIN);
        if (shopDomain != null) {
            record.put(CONTEXT_PREFIX + "shop_domain", shopDomain);
        }
    }

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

    // --- Change Type Mapping ---

    static String mapActionToChangeType(String action) {
        if (action == null) return "UNKNOWN";
        switch (action.toLowerCase()) {
            case "create":
                return "CREATE";
            case "update":
            case "updated":
                return "UPDATE";
            case "delete":
                return "DELETE";
            case "cancelled":
                return "CANCELLED";
            case "fulfilled":
                return "FULFILLED";
            case "paid":
                return "PAID";
            case "partially_fulfilled":
                return "PARTIALLY_FULFILLED";
            default:
                return action.toUpperCase();
        }
    }

    static boolean isDeleteAction(String action) {
        return action != null && "delete".equalsIgnoreCase(action);
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

    // --- HMAC Verification ---

    private void verifyHmac(Map<String, Object> headers) {
        String providedHmac = getHeaderString(headers, HEADER_HMAC);
        if (providedHmac == null) {
            throw new PayloadRoutingException(
                    "Shopify HMAC verification enabled but X-Shopify-Hmac-Sha256 header missing");
        }

        Object rawBodyObj = headers.get("__rawBody");
        if (rawBodyObj == null) {
            throw new PayloadRoutingException(
                    "Shopify HMAC verification requires raw body but __rawBody not found in headers");
        }
        String rawBody = String.valueOf(rawBodyObj);

        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(
                    hmacSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKey);
            byte[] hash = mac.doFinal(rawBody.getBytes(StandardCharsets.UTF_8));
            String computedHmac = Base64.getEncoder().encodeToString(hash);

            if (!computedHmac.equals(providedHmac)) {
                throw new PayloadRoutingException("Shopify HMAC verification failed");
            }
            LOG.debug("Shopify HMAC verification passed");
        } catch (PayloadRoutingException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadRoutingException("Shopify HMAC verification error", e);
        }
    }

    // --- Helpers ---

    private List<RoutedRecord> handleUnknownType(Map<String, Object> payload, String eventType) {
        switch (unknownTypeBehavior) {
            case SKIP:
                LOG.warn("Skipping unknown Shopify event: {}", eventType);
                return Collections.emptyList();
            case FAIL:
                throw new PayloadRoutingException("Unknown Shopify event: " + eventType);
            case DEFAULT_TOPIC:
            default:
                LOG.info("Routing unknown Shopify event '{}' to default topic: {}", eventType, defaultTopic);
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

    private static Map<String, Object> keyOf(String field1, Object value1, String field2, Object value2) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put(field1, value1);
        key.put(field2, value2);
        return key;
    }

    private static String getHeaderString(Map<String, Object> headers, String key) {
        Object value = headers.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    private static void putIfHeaderPresent(Map<String, Object> target, String targetKey,
                                           Map<String, Object> headers, String headerKey) {
        String value = getHeaderString(headers, headerKey);
        if (value != null) {
            target.put(targetKey, value);
        }
    }
}
