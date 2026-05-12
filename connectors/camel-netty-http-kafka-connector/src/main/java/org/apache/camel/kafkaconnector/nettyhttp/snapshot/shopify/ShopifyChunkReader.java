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

    // Map object names to GraphQL connection fields, node fields, and count queries
    private static final Map<String, ObjectConfig> OBJECT_CONFIGS = new LinkedHashMap<>();
    static {
        OBJECT_CONFIGS.put("orders", new ObjectConfig("orders", "ordersCount",
                "id name email createdAt updatedAt totalPriceSet { shopMoney { amount currencyCode } } "
                + "displayFinancialStatus displayFulfillmentStatus cancelledAt closedAt "
                + "customer { id email } "
                + "lineItems(first: 50) { edges { node { id title quantity sku "
                + "originalUnitPriceSet { shopMoney { amount currencyCode } } } } }"));
        OBJECT_CONFIGS.put("products", new ObjectConfig("products", "productsCount",
                "id title handle status vendor productType createdAt updatedAt "
                + "variants(first: 50) { edges { node { id title sku price inventoryQuantity } } } "
                + "images(first: 10) { edges { node { id url altText } } }"));
        OBJECT_CONFIGS.put("customers", new ObjectConfig("customers", "customersCount",
                "id firstName lastName email phone createdAt updatedAt state numberOfOrders "
                + "addresses(first: 10) { address1 address2 city province country zip }"));
        OBJECT_CONFIGS.put("draft_orders", new ObjectConfig("draftOrders", "draftOrdersCount",
                "id name status createdAt updatedAt "
                + "lineItems(first: 50) { edges { node { id title quantity "
                + "originalUnitPriceSet { shopMoney { amount currencyCode } } } } }"));
        OBJECT_CONFIGS.put("collections", new ObjectConfig("collections", "collectionsCount",
                "id title handle updatedAt sortOrder"));
        OBJECT_CONFIGS.put("inventory_items", new ObjectConfig("inventoryItems", null,
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

        // Build ID-based filter from afterKey/endKey.
        // The SnapshotCoordinator passes numeric IDs (not opaque cursors),
        // so we use Shopify's query filter (id:>X) instead of cursor-based after:.
        String idFilter = buildIdFilter(afterKey, endKey);
        String combinedCondition = combineConditions(idFilter, additionalCondition);

        String cursor = null; // Use cursor only for internal multi-page reads within a chunk
        int remaining = chunkSize;

        while (remaining > 0) {
            int pageSize = Math.min(remaining, SHOPIFY_MAX_PAGE_SIZE);

            String query = buildPageQuery(config, pageSize, cursor, combinedCondition);
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
                    allResults.add(flattenNode(node));
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

        // Use Shopify's dedicated count query (productsCount, ordersCount, etc.)
        if (config.countQuery != null) {
            String queryFilter = additionalCondition != null && !additionalCondition.isEmpty()
                    ? "(query: \"" + escapeGraphql(additionalCondition) + "\")"
                    : "";
            String query = "{ " + config.countQuery + queryFilter + " { count precision } }";

            String requestBody = objectMapper.writeValueAsString(Map.of("query", query));
            HttpResponse<String> response = authClient.executeGraphql(requestBody);
            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

            Map<String, Object> data = (Map<String, Object>) result.get("data");
            if (data != null) {
                Map<String, Object> countResult = (Map<String, Object>) data.get(config.countQuery);
                if (countResult != null && countResult.get("count") instanceof Number) {
                    long count = ((Number) countResult.get("count")).longValue();
                    String precision = (String) countResult.get("precision");
                    LOG.info("Shopify {} count: {} (precision: {})", objectName, count, precision);
                    return count;
                }
            }
        }

        // Fallback for objects without a count query (e.g., inventory_items)
        LOG.info("No count query for {}, estimating from first page", objectName);
        String firstQuery = "{ " + config.connectionField + "(first: 1, sortKey: ID) { "
                + "edges { node { id } } pageInfo { hasNextPage } } }";
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

        return 1000; // Unknown, assume moderate size
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
     * Build Shopify query filter for ID-based pagination.
     * The SnapshotCoordinator passes numeric IDs, which we translate to Shopify query syntax.
     * e.g., afterKey="100", endKey="200" → "id:>100 AND id:<200"
     */
    private static String buildIdFilter(String afterKey, String endKey) {
        List<String> parts = new ArrayList<>();
        if (afterKey != null && !afterKey.isEmpty()) {
            parts.add("id:>" + extractNumericId(afterKey));
        }
        if (endKey != null && !endKey.isEmpty()) {
            parts.add("id:<" + extractNumericId(endKey));
        }
        return parts.isEmpty() ? null : String.join(" AND ", parts);
    }

    /**
     * Combine an ID filter with an additional user-provided condition.
     */
    private static String combineConditions(String idFilter, String additionalCondition) {
        boolean hasId = idFilter != null && !idFilter.isEmpty();
        boolean hasAdditional = additionalCondition != null && !additionalCondition.isEmpty();
        if (hasId && hasAdditional) {
            return idFilter + " AND " + additionalCondition;
        }
        if (hasId) return idFilter;
        if (hasAdditional) return additionalCondition;
        return null;
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
        final String countQuery;       // Dedicated count query (e.g., "productsCount"), null if unavailable
        final String nodeFields;       // Fields to select on each node

        ObjectConfig(String connectionField, String countQuery, String nodeFields) {
            this.connectionField = connectionField;
            this.countQuery = countQuery;
            this.nodeFields = nodeFields;
        }
    }
}
