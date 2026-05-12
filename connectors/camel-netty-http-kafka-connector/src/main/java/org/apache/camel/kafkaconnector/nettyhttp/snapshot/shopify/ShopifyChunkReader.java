package org.apache.camel.kafkaconnector.nettyhttp.snapshot.shopify;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.ChunkReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads chunks from Shopify using GraphQL Admin API with cursor-based pagination.
 * Shopify limits connections to 250 items per page, so readChunk() internally
 * pages through multiple requests when chunkSize > 250.
 *
 * Supported object types: orders, products, customers, draft_orders,
 * collections, inventory_items.
 */
public class ShopifyChunkReader implements ChunkReader {

    private static final Logger LOG = LoggerFactory.getLogger(ShopifyChunkReader.class);
    private static final int SHOPIFY_MAX_PAGE_SIZE = 250;

    private final ShopifyAuthClient authClient;
    private final ObjectMapper objectMapper;

    // Map object names to GraphQL connection fields and their node fields
    private static final Map<String, ObjectConfig> OBJECT_CONFIGS = new LinkedHashMap<>();
    static {
        OBJECT_CONFIGS.put("orders", new ObjectConfig("orders",
                "id name email createdAt updatedAt totalPriceSet { shopMoney { amount currencyCode } } "
                + "displayFinancialStatus displayFulfillmentStatus cancelledAt closedAt "
                + "customer { id email } "
                + "lineItems(first: 50) { edges { node { id title quantity sku "
                + "originalUnitPriceSet { shopMoney { amount currencyCode } } } } }"));
        OBJECT_CONFIGS.put("products", new ObjectConfig("products",
                "id title handle status vendor productType createdAt updatedAt "
                + "variants(first: 50) { edges { node { id title sku price inventoryQuantity } } } "
                + "images(first: 10) { edges { node { id url altText } } }"));
        OBJECT_CONFIGS.put("customers", new ObjectConfig("customers",
                "id firstName lastName email phone createdAt updatedAt state numberOfOrders "
                + "addresses(first: 10) { address1 address2 city province country zip }"));
        OBJECT_CONFIGS.put("draft_orders", new ObjectConfig("draftOrders",
                "id name status createdAt updatedAt "
                + "lineItems(first: 50) { edges { node { id title quantity "
                + "originalUnitPriceSet { shopMoney { amount currencyCode } } } } }"));
        OBJECT_CONFIGS.put("collections", new ObjectConfig("collections",
                "id title handle updatedAt sortOrder"));
        OBJECT_CONFIGS.put("inventory_items", new ObjectConfig("inventoryItems",
                "id sku createdAt updatedAt requiresShipping tracked"));
    }

    public ShopifyChunkReader(ShopifyAuthClient authClient) {
        this.authClient = authClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readChunk(String objectName, String afterKey, String endKey,
                                                int chunkSize, String additionalCondition) throws Exception {
        ObjectConfig config = getObjectConfig(objectName);
        List<Map<String, Object>> allResults = new ArrayList<>();
        String cursor = afterKey;
        int remaining = chunkSize;

        while (remaining > 0) {
            int pageSize = Math.min(remaining, SHOPIFY_MAX_PAGE_SIZE);

            String query = buildPageQuery(config, pageSize, cursor, additionalCondition);
            String requestBody = objectMapper.writeValueAsString(Map.of("query", query));

            LOG.debug("Shopify readChunk: object={}, cursor={}, pageSize={}", objectName, cursor, pageSize);

            HttpResponse<String> response = authClient.executeGraphql(requestBody);
            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

            // Check for GraphQL errors
            List<Map<String, Object>> errors = (List<Map<String, Object>>) result.get("errors");
            if (errors != null && !errors.isEmpty()) {
                throw new RuntimeException("Shopify GraphQL error: " + errors.get(0).get("message"));
            }

            Map<String, Object> data = (Map<String, Object>) result.get("data");
            if (data == null) {
                break;
            }

            Map<String, Object> connection = (Map<String, Object>) data.get(config.connectionField);
            if (connection == null) {
                break;
            }

            List<Map<String, Object>> edges = (List<Map<String, Object>>) connection.get("edges");
            if (edges == null || edges.isEmpty()) {
                break;
            }

            for (Map<String, Object> edge : edges) {
                Map<String, Object> node = (Map<String, Object>) edge.get("node");
                if (node != null) {
                    Map<String, Object> flattened = flattenNode(node);

                    // Filter by endKey if provided (Id < endKey)
                    if (endKey != null) {
                        String nodeId = extractNumericId((String) flattened.get("id"));
                        String endNumeric = extractNumericId(endKey);
                        if (nodeId != null && endNumeric != null) {
                            if (Long.parseLong(nodeId) >= Long.parseLong(endNumeric)) {
                                return allResults;
                            }
                        }
                    }

                    allResults.add(flattened);
                }
                cursor = (String) edge.get("cursor");
            }

            remaining -= edges.size();

            // Check if there are more pages
            Map<String, Object> pageInfo = (Map<String, Object>) connection.get("pageInfo");
            if (pageInfo == null || !Boolean.TRUE.equals(pageInfo.get("hasNextPage"))) {
                break;
            }

            if (edges.size() < pageSize) {
                break;
            }
        }

        return allResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getMaxKey(String objectName, String additionalCondition) throws Exception {
        ObjectConfig config = getObjectConfig(objectName);

        String queryFilter = additionalCondition != null && !additionalCondition.isEmpty()
                ? ", query: \"" + escapeGraphql(additionalCondition) + "\""
                : "";
        String query = "{ " + config.connectionField + "(last: 1, sortKey: ID" + queryFilter + ") { "
                + "edges { node { id } } } }";

        String requestBody = objectMapper.writeValueAsString(Map.of("query", query));
        HttpResponse<String> response = authClient.executeGraphql(requestBody);
        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

        Map<String, Object> data = (Map<String, Object>) result.get("data");
        if (data == null) return null;

        Map<String, Object> connection = (Map<String, Object>) data.get(config.connectionField);
        if (connection == null) return null;

        List<Map<String, Object>> edges = (List<Map<String, Object>>) connection.get("edges");
        if (edges == null || edges.isEmpty()) return null;

        Map<String, Object> node = (Map<String, Object>) edges.get(0).get("node");
        return node != null ? extractNumericId((String) node.get("id")) : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public long getApproximateCount(String objectName, String additionalCondition) throws Exception {
        ObjectConfig config = getObjectConfig(objectName);

        String queryFilter = additionalCondition != null && !additionalCondition.isEmpty()
                ? "(query: \"" + escapeGraphql(additionalCondition) + "\")"
                : "";
        String query = "{ " + config.connectionField + queryFilter + " { edges { cursor } pageInfo { hasNextPage } } }";

        // Use a count query — Shopify exposes count on some connections
        String countQuery = "{ " + config.connectionField + queryFilter + " { edges { cursor } } }";

        // Simpler approach: use the count field if available, otherwise estimate
        // Most Shopify connections support a count via first:1 with totalCount isn't available
        // We'll use a rough estimate based on cursor-hopping
        String firstQuery = "{ " + config.connectionField + "(first: 1" +
                (additionalCondition != null && !additionalCondition.isEmpty()
                        ? ", query: \"" + escapeGraphql(additionalCondition) + "\""
                        : "")
                + ", sortKey: ID) { edges { node { id } cursor } pageInfo { hasNextPage } } }";

        String requestBody = objectMapper.writeValueAsString(Map.of("query", firstQuery));
        HttpResponse<String> response = authClient.executeGraphql(requestBody);
        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

        Map<String, Object> data = (Map<String, Object>) result.get("data");
        if (data == null) return 0;

        Map<String, Object> connection = (Map<String, Object>) data.get(config.connectionField);
        if (connection == null) return 0;

        List<Map<String, Object>> edges = (List<Map<String, Object>>) connection.get("edges");
        if (edges == null || edges.isEmpty()) return 0;

        Map<String, Object> pageInfo = (Map<String, Object>) connection.get("pageInfo");
        if (pageInfo == null || !Boolean.TRUE.equals(pageInfo.get("hasNextPage"))) {
            return edges.size();
        }

        // Estimate: count by paging through with 250/page, counting cursors
        // For large datasets this is expensive, so use the numeric ID range as estimate
        String minId = extractNumericId((String) ((Map<String, Object>) edges.get(0).get("node")).get("id"));
        String maxId = getMaxKey(objectName, additionalCondition);

        if (minId != null && maxId != null) {
            // Rough estimate based on ID range — not exact but good enough for segment decisions
            long range = Long.parseLong(maxId) - Long.parseLong(minId);
            // Shopify IDs are roughly sequential, so range ~ count
            return Math.max(range, 1);
        }

        return 1000; // Default estimate if we can't determine
    }

    @Override
    public List<String> getSegmentBoundaries(String objectName, int numSegments,
                                              String additionalCondition) throws Exception {
        if (numSegments <= 1) {
            return Collections.emptyList();
        }

        long count = getApproximateCount(objectName, additionalCondition);
        if (count == 0) {
            return Collections.emptyList();
        }

        String minId = getMinKey(objectName, additionalCondition);
        String maxId = getMaxKey(objectName, additionalCondition);

        if (minId == null || maxId == null || minId.equals(maxId)) {
            return Collections.emptyList();
        }

        // Shopify numeric IDs are sequential, so we can split the range evenly
        long min = Long.parseLong(minId);
        long max = Long.parseLong(maxId);
        long step = (max - min) / numSegments;

        if (step == 0) {
            return Collections.emptyList();
        }

        List<String> boundaries = new ArrayList<>();
        for (int i = 1; i < numSegments; i++) {
            boundaries.add(String.valueOf(min + step * i));
        }

        LOG.info("Segment boundaries for {} ({} segments): {}", objectName, numSegments, boundaries);
        return boundaries;
    }

    @Override
    public String getIdFieldName() {
        return "id";
    }

    @Override
    public void close() {
        // HttpClient doesn't need explicit close
    }

    // --- Internal ---

    @SuppressWarnings("unchecked")
    private String getMinKey(String objectName, String additionalCondition) throws Exception {
        ObjectConfig config = getObjectConfig(objectName);

        String queryFilter = additionalCondition != null && !additionalCondition.isEmpty()
                ? ", query: \"" + escapeGraphql(additionalCondition) + "\""
                : "";
        String query = "{ " + config.connectionField + "(first: 1, sortKey: ID" + queryFilter + ") { "
                + "edges { node { id } } } }";

        String requestBody = objectMapper.writeValueAsString(Map.of("query", query));
        HttpResponse<String> response = authClient.executeGraphql(requestBody);
        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

        Map<String, Object> data = (Map<String, Object>) result.get("data");
        if (data == null) return null;

        Map<String, Object> connection = (Map<String, Object>) data.get(config.connectionField);
        if (connection == null) return null;

        List<Map<String, Object>> edges = (List<Map<String, Object>>) connection.get("edges");
        if (edges == null || edges.isEmpty()) return null;

        Map<String, Object> node = (Map<String, Object>) edges.get(0).get("node");
        return node != null ? extractNumericId((String) node.get("id")) : null;
    }

    private String buildPageQuery(ObjectConfig config, int pageSize, String cursor,
                                   String additionalCondition) {
        StringBuilder args = new StringBuilder();
        args.append("first: ").append(pageSize);
        args.append(", sortKey: ID");

        if (cursor != null && !cursor.isEmpty()) {
            args.append(", after: \"").append(cursor).append("\"");
        }
        if (additionalCondition != null && !additionalCondition.isEmpty()) {
            args.append(", query: \"").append(escapeGraphql(additionalCondition)).append("\"");
        }

        return "{ " + config.connectionField + "(" + args + ") { "
                + "edges { node { " + config.nodeFields + " } cursor } "
                + "pageInfo { hasNextPage } } }";
    }

    /**
     * Flatten a GraphQL node into a simple key-value map.
     * Converts nested connections (edges/node) into lists of maps.
     * Converts Shopify GID (gid://shopify/Order/12345) to numeric ID.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenNode(Map<String, Object> node) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if ("id".equals(key) && value instanceof String) {
                // Convert GID to numeric: gid://shopify/Order/12345 → 12345
                result.put(key, extractNumericId((String) value));
            } else if (value instanceof Map) {
                Map<String, Object> nested = (Map<String, Object>) value;
                if (nested.containsKey("edges")) {
                    // This is a connection — flatten to list of node maps
                    List<Map<String, Object>> items = new ArrayList<>();
                    List<Map<String, Object>> edges = (List<Map<String, Object>>) nested.get("edges");
                    if (edges != null) {
                        for (Map<String, Object> edge : edges) {
                            Map<String, Object> childNode = (Map<String, Object>) edge.get("node");
                            if (childNode != null) {
                                items.add(flattenNode(childNode));
                            }
                        }
                    }
                    result.put(key, items);
                } else if (nested.containsKey("shopMoney")) {
                    // Money field — flatten to amount + currency
                    Map<String, Object> money = (Map<String, Object>) nested.get("shopMoney");
                    result.put(key, money.get("amount"));
                    result.put(key + "Currency", money.get("currencyCode"));
                } else if (nested.containsKey("id")) {
                    // Nested single object (like customer) — flatten with prefix
                    Map<String, Object> flat = flattenNode(nested);
                    for (Map.Entry<String, Object> nestedEntry : flat.entrySet()) {
                        result.put(key + "_" + nestedEntry.getKey(), nestedEntry.getValue());
                    }
                } else {
                    result.put(key, nested);
                }
            } else {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * Extract numeric ID from Shopify GID.
     * "gid://shopify/Order/12345" → "12345"
     * "12345" → "12345" (already numeric)
     */
    static String extractNumericId(String gid) {
        if (gid == null) return null;
        int lastSlash = gid.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < gid.length() - 1) {
            return gid.substring(lastSlash + 1);
        }
        return gid;
    }

    private ObjectConfig getObjectConfig(String objectName) {
        ObjectConfig config = OBJECT_CONFIGS.get(objectName.toLowerCase());
        if (config == null) {
            throw new RuntimeException("Unsupported Shopify object for snapshot: '" + objectName
                    + "'. Supported: " + OBJECT_CONFIGS.keySet());
        }
        return config;
    }

    private static String escapeGraphql(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Configuration for a Shopify object type.
     */
    static class ObjectConfig {
        final String connectionField;  // GraphQL connection name (e.g., "orders")
        final String nodeFields;       // Fields to select on each node

        ObjectConfig(String connectionField, String nodeFields) {
            this.connectionField = connectionField;
            this.nodeFields = nodeFields;
        }
    }
}
