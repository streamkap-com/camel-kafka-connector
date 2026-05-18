package org.apache.camel.kafkaconnector.nettyhttp.snapshot.stripe;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
 * Reads chunks from Stripe using REST List APIs with cursor-based pagination.
 * Stripe limits list requests to 100 items per page, so readChunk() pages
 * internally when chunkSize > 100.
 */
public class StripeChunkReader implements ChunkReader {

    private static final Logger LOG = LoggerFactory.getLogger(StripeChunkReader.class);
    private static final int STRIPE_MAX_PAGE_SIZE = 100;

    private final StripeAuthClient authClient;
    private final ObjectMapper objectMapper;

    // Map object names to Stripe API endpoints
    private static final Map<String, String> OBJECT_ENDPOINTS = new LinkedHashMap<>();
    static {
        OBJECT_ENDPOINTS.put("customer", "/v1/customers");
        OBJECT_ENDPOINTS.put("charge", "/v1/charges");
        OBJECT_ENDPOINTS.put("payment_intent", "/v1/payment_intents");
        OBJECT_ENDPOINTS.put("invoice", "/v1/invoices");
        OBJECT_ENDPOINTS.put("subscription", "/v1/subscriptions");
        OBJECT_ENDPOINTS.put("product", "/v1/products");
        OBJECT_ENDPOINTS.put("price", "/v1/prices");
        OBJECT_ENDPOINTS.put("payout", "/v1/payouts");
        OBJECT_ENDPOINTS.put("refund", "/v1/refunds");
        OBJECT_ENDPOINTS.put("payment_method", "/v1/payment_methods");
        OBJECT_ENDPOINTS.put("balance_transaction", "/v1/balance_transactions");
        OBJECT_ENDPOINTS.put("coupon", "/v1/coupons");
        OBJECT_ENDPOINTS.put("plan", "/v1/plans");
        OBJECT_ENDPOINTS.put("setup_intent", "/v1/setup_intents");
    }

    public StripeChunkReader(StripeAuthClient authClient) {
        this.authClient = authClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readChunk(String objectName, String afterKey, String endKey,
                                                int chunkSize, String additionalCondition) throws Exception {
        String endpoint = getEndpoint(objectName);
        List<Map<String, Object>> allResults = new ArrayList<>();
        String cursor = afterKey;
        int remaining = chunkSize;

        while (remaining > 0) {
            int pageSize = Math.min(remaining, STRIPE_MAX_PAGE_SIZE);

            StringBuilder path = new StringBuilder(endpoint);
            path.append("?limit=").append(pageSize);

            if (cursor != null && !cursor.isEmpty()) {
                path.append("&starting_after=").append(encode(cursor));
            }

            if (additionalCondition != null && !additionalCondition.isEmpty()) {
                // additionalCondition is already in Stripe query param format
                // e.g., "created[gt]=1680000000" or "status=active"
                path.append("&").append(additionalCondition);
            }

            LOG.debug("Stripe readChunk: object={}, cursor={}, pageSize={}", objectName, cursor, pageSize);

            HttpResponse<String> response = authClient.executeGet(path.toString());
            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

            List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
            if (data == null || data.isEmpty()) {
                break;
            }

            // Filter by endKey if provided
            for (Map<String, Object> record : data) {
                String recordId = (String) record.get("id");
                if (endKey != null && recordId != null && recordId.compareTo(endKey) >= 0) {
                    return allResults;
                }
                allResults.add(record);
            }

            cursor = (String) data.get(data.size() - 1).get("id");
            remaining -= data.size();

            Boolean hasMore = (Boolean) result.get("has_more");
            if (!Boolean.TRUE.equals(hasMore)) {
                break;
            }

            if (data.size() < pageSize) {
                break;
            }
        }

        return allResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getMaxKey(String objectName, String additionalCondition) throws Exception {
        // Stripe lists are ordered by creation date descending by default
        // Get the first item (newest) — but we actually need the last/oldest for max key
        // Stripe doesn't support reverse ordering, so we just return null
        // The coordinator handles null maxKey gracefully
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public long getApproximateCount(String objectName, String additionalCondition) throws Exception {
        // Stripe doesn't have a count endpoint
        // Fetch first page with limit=1 to check if data exists
        String endpoint = getEndpoint(objectName);
        StringBuilder path = new StringBuilder(endpoint);
        path.append("?limit=1");

        if (additionalCondition != null && !additionalCondition.isEmpty()) {
            path.append("&").append(additionalCondition);
        }

        HttpResponse<String> response = authClient.executeGet(path.toString());
        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);

        List<?> data = (List<?>) result.get("data");
        if (data == null || data.isEmpty()) {
            LOG.info("Stripe {} count: 0", objectName);
            return 0;
        }

        Boolean hasMore = (Boolean) result.get("has_more");
        if (!Boolean.TRUE.equals(hasMore)) {
            LOG.info("Stripe {} count: {} (exact, single page)", objectName, data.size());
            return data.size();
        }

        // Has more data — return conservative estimate
        // Stripe doesn't tell us the total count
        LOG.info("Stripe {} count: unknown (has_more=true), using estimate 1000", objectName);
        return 1000;
    }

    @Override
    public List<String> getSegmentBoundaries(String objectName, int numSegments,
                                              String additionalCondition) throws Exception {
        // Stripe IDs are not numeric or sortable in a useful range
        // Can't split into parallel segments
        return Collections.emptyList();
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

    private String getEndpoint(String objectName) {
        String endpoint = OBJECT_ENDPOINTS.get(objectName.toLowerCase());
        if (endpoint == null) {
            throw new RuntimeException("Unsupported Stripe object for snapshot: '" + objectName
                    + "'. Supported: " + OBJECT_ENDPOINTS.keySet());
        }
        return endpoint;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
