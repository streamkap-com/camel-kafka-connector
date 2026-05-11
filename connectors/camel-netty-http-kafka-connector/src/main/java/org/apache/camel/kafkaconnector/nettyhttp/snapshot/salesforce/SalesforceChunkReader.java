package org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.ChunkReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads chunks from Salesforce using SOQL REST API.
 * Uses Id-based pagination: WHERE Id > :lastKey ORDER BY Id LIMIT :chunkSize
 */
public class SalesforceChunkReader implements ChunkReader {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceChunkReader.class);
    private static final String API_VERSION = "v59.0";

    private final SalesforceAuthClient authClient;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public SalesforceChunkReader(SalesforceAuthClient authClient) {
        this.authClient = authClient;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    // Salesforce FIELDS(ALL) requires LIMIT <= 200
    private static final int FIELDS_ALL_MAX_LIMIT = 200;

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> readChunk(String objectName, String afterKey, String endKey,
                                                int chunkSize, String additionalCondition) throws Exception {
        // FIELDS(ALL) is limited to 200 rows per query, so we page internally
        List<Map<String, Object>> allResults = new ArrayList<>();
        String currentAfterKey = afterKey;
        int remaining = chunkSize;

        while (remaining > 0) {
            int pageSize = Math.min(remaining, FIELDS_ALL_MAX_LIMIT);

            StringBuilder soql = new StringBuilder();
            soql.append("SELECT FIELDS(ALL) FROM ").append(objectName);

            List<String> conditions = new ArrayList<>();
            if (currentAfterKey != null) {
                conditions.add("Id > '" + escapeSOQL(currentAfterKey) + "'");
            }
            if (endKey != null) {
                conditions.add("Id < '" + escapeSOQL(endKey) + "'");
            }
            if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
                conditions.add("(" + additionalCondition + ")");
            }

            if (!conditions.isEmpty()) {
                soql.append(" WHERE ").append(String.join(" AND ", conditions));
            }

            soql.append(" ORDER BY Id ASC LIMIT ").append(pageSize);

            List<Map<String, Object>> page = executeSoqlQuery(soql.toString());
            if (page.isEmpty()) {
                break;
            }

            allResults.addAll(page);
            currentAfterKey = (String) page.get(page.size() - 1).get("Id");
            remaining -= page.size();

            // If we got fewer than requested, no more data
            if (page.size() < pageSize) {
                break;
            }
        }

        return allResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getMaxKey(String objectName, String additionalCondition) throws Exception {
        StringBuilder soql = new StringBuilder();
        soql.append("SELECT Id FROM ").append(objectName);

        if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
            soql.append(" WHERE ").append(additionalCondition);
        }

        soql.append(" ORDER BY Id DESC LIMIT 1");

        List<Map<String, Object>> results = executeSoqlQuery(soql.toString());
        if (results.isEmpty()) {
            return null;
        }
        return (String) results.get(0).get("Id");
    }

    @Override
    @SuppressWarnings("unchecked")
    public long getApproximateCount(String objectName, String additionalCondition) throws Exception {
        StringBuilder soql = new StringBuilder();
        soql.append("SELECT COUNT() FROM ").append(objectName);

        if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
            soql.append(" WHERE ").append(additionalCondition);
        }

        String instanceUrl = authClient.getTokenInstanceUrl();
        String token = authClient.getAccessToken();
        String encodedSoql = java.net.URLEncoder.encode(soql.toString(), java.nio.charset.StandardCharsets.UTF_8);
        String url = instanceUrl + "/services/data/" + API_VERSION + "/query/?q=" + encodedSoql;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        handleErrorResponse(response);

        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
        Object totalSize = result.get("totalSize");
        return totalSize instanceof Number ? ((Number) totalSize).longValue() : 0;
    }

    // Max rows Salesforce returns in a single SOQL query (for SELECT Id ... LIMIT N)
    private static final int SOQL_MAX_ROWS = 2000;
    private static final double SKEW_THRESHOLD = 2.0;

    @Override
    @SuppressWarnings("unchecked")
    public List<String> getSegmentBoundaries(String objectName, int numSegments,
                                              String additionalCondition) throws Exception {
        if (numSegments <= 1) {
            return Collections.emptyList();
        }

        // Step 1: Get count, min, max (3 API calls)
        long totalCount = getApproximateCount(objectName, additionalCondition);
        if (totalCount == 0) {
            return Collections.emptyList();
        }

        String minKey = getMinKey(objectName, additionalCondition);
        String maxKey = getMaxKey(objectName, additionalCondition);
        if (minKey == null || maxKey == null || minKey.equals(maxKey)) {
            return Collections.emptyList();
        }

        // Step 2: Calculate segment size
        long segmentSize = totalCount / numSegments;
        if (segmentSize == 0) {
            return Collections.emptyList();
        }

        List<String> boundaries;
        if (segmentSize <= SOQL_MAX_ROWS) {
            // Direct approach: hop through data one segment at a time.
            // Each boundary found in 1 API call. Fast and exact.
            boundaries = getBoundariesDirect(objectName, numSegments, segmentSize, additionalCondition);
        } else {
            // Large table: lexicographic split then validate + rebalance.
            // Avoids reading millions of Ids.
            boundaries = splitIdRange(minKey, maxKey, numSegments);
            boundaries = rebalanceIfSkewed(objectName, minKey, maxKey, boundaries, numSegments, additionalCondition);
        }

        LOG.info("Segment boundaries for {} ({} segments, {} rows): {}",
                objectName, boundaries.size() + 1, totalCount, boundaries);
        return boundaries;
    }

    /**
     * Direct boundary finding: for each boundary, read segmentSize Ids starting from
     * the previous boundary and take the last Id. Exact and efficient when segmentSize <= 2000.
     * API calls: numSegments - 1 (one per boundary, last segment uses maxKey)
     */
    @SuppressWarnings("unchecked")
    private List<String> getBoundariesDirect(String objectName, int numSegments, long segmentSize,
                                              String additionalCondition) throws Exception {
        List<String> boundaries = new ArrayList<>();
        String lastBoundary = null;

        for (int i = 1; i < numSegments; i++) {
            StringBuilder soql = new StringBuilder();
            soql.append("SELECT Id FROM ").append(objectName);

            List<String> conditions = new ArrayList<>();
            if (lastBoundary != null) {
                conditions.add("Id > '" + escapeSOQL(lastBoundary) + "'");
            }
            if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
                conditions.add("(" + additionalCondition + ")");
            }
            if (!conditions.isEmpty()) {
                soql.append(" WHERE ").append(String.join(" AND ", conditions));
            }
            soql.append(" ORDER BY Id ASC LIMIT ").append(segmentSize);

            List<Map<String, Object>> results = executeSoqlQuery(soql.toString());
            if (results.isEmpty()) {
                break;
            }

            lastBoundary = (String) results.get(results.size() - 1).get("Id");
            boundaries.add(lastBoundary);
        }

        return boundaries;
    }

    /**
     * For large tables: validate lexicographic boundaries using COUNT() per segment.
     * If skewed (any segment >2x average), rebalance by interpolating within the
     * heavy segments proportionally to their counts.
     * API calls: numSegments (one COUNT per segment)
     */
    private List<String> rebalanceIfSkewed(String objectName, String minKey, String maxKey,
                                            List<String> boundaries, int targetSegments,
                                            String additionalCondition) throws Exception {
        List<Long> counts = countPerSegment(objectName, minKey, maxKey, boundaries, additionalCondition);
        long totalRows = counts.stream().mapToLong(Long::longValue).sum();

        if (totalRows == 0) {
            return boundaries;
        }

        long avgPerSegment = totalRows / counts.size();
        boolean skewed = counts.stream().anyMatch(c -> c > avgPerSegment * SKEW_THRESHOLD);

        if (!skewed) {
            LOG.debug("Boundaries balanced, counts: {}", counts);
            return boundaries;
        }

        LOG.info("Boundaries skewed, rebalancing. Counts: {}, avg: {}", counts, avgPerSegment);

        // Place new boundaries where cumulative count crosses target thresholds
        List<String> allPoints = new ArrayList<>();
        allPoints.add(minKey);
        allPoints.addAll(boundaries);
        allPoints.add(maxKey);

        long targetPerSegment = totalRows / targetSegments;
        List<String> newBoundaries = new ArrayList<>();
        long cumulative = 0;
        int nextBoundary = 1;

        for (int i = 0; i < counts.size() && nextBoundary < targetSegments; i++) {
            long segStart = cumulative;
            long segCount = counts.get(i);
            cumulative += segCount;

            String rangeStart = allPoints.get(i);
            String rangeEnd = allPoints.get(i + 1);

            while (nextBoundary < targetSegments && nextBoundary * targetPerSegment <= cumulative) {
                long positionInSegment = nextBoundary * targetPerSegment - segStart;
                if (segCount > 0) {
                    String boundary = interpolateId(rangeStart, rangeEnd, (int) positionInSegment, (int) segCount);
                    newBoundaries.add(boundary);
                } else {
                    newBoundaries.add(rangeEnd);
                }
                nextBoundary++;
            }
        }

        return newBoundaries;
    }

    private List<Long> countPerSegment(String objectName, String minKey, String maxKey,
                                        List<String> boundaries, String additionalCondition) throws Exception {
        List<Long> counts = new ArrayList<>();

        List<String> allPoints = new ArrayList<>();
        allPoints.add(minKey);
        allPoints.addAll(boundaries);
        allPoints.add(maxKey);

        for (int i = 0; i < allPoints.size() - 1; i++) {
            String from = allPoints.get(i);
            String to = allPoints.get(i + 1);
            boolean isLast = (i == allPoints.size() - 2);

            StringBuilder soql = new StringBuilder();
            soql.append("SELECT COUNT() FROM ").append(objectName);
            soql.append(" WHERE Id >= '").append(escapeSOQL(from)).append("'");
            soql.append(isLast ? " AND Id <= '" : " AND Id < '").append(escapeSOQL(to)).append("'");
            if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
                soql.append(" AND (").append(additionalCondition).append(")");
            }

            String instanceUrl = authClient.getTokenInstanceUrl();
            String token = authClient.getAccessToken();
            String encodedSoql = java.net.URLEncoder.encode(soql.toString(), java.nio.charset.StandardCharsets.UTF_8);
            String url = instanceUrl + "/services/data/" + API_VERSION + "/query/?q=" + encodedSoql;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + token)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            handleErrorResponse(response);

            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
            Object totalSize = result.get("totalSize");
            counts.add(totalSize instanceof Number ? ((Number) totalSize).longValue() : 0);
        }

        return counts;
    }

    @SuppressWarnings("unchecked")
    private String getMinKey(String objectName, String additionalCondition) throws Exception {
        StringBuilder soql = new StringBuilder();
        soql.append("SELECT Id FROM ").append(objectName);
        if (additionalCondition != null && !additionalCondition.trim().isEmpty()) {
            soql.append(" WHERE ").append(additionalCondition);
        }
        soql.append(" ORDER BY Id ASC LIMIT 1");

        List<Map<String, Object>> results = executeSoqlQuery(soql.toString());
        return results.isEmpty() ? null : (String) results.get(0).get("Id");
    }

    /**
     * Split an Id range into numSegments by interpolating between min and max lexicographically.
     * Returns numSegments-1 boundary keys.
     */
    static List<String> splitIdRange(String minId, String maxId, int numSegments) {
        if (numSegments <= 1 || minId == null || maxId == null) {
            return Collections.emptyList();
        }

        // Ensure same length (Salesforce Ids are always 15 or 18 chars)
        int len = Math.min(minId.length(), maxId.length());
        String min = minId.substring(0, len);
        String max = maxId.substring(0, len);

        List<String> boundaries = new ArrayList<>();
        for (int i = 1; i < numSegments; i++) {
            String boundary = interpolateId(min, max, i, numSegments);
            boundaries.add(boundary);
        }
        return boundaries;
    }

    /**
     * Interpolate between two Id strings: result = min + (max - min) * position / total.
     * Uses character-by-character interpolation treating each char as a digit in base-128 (ASCII).
     */
    private static String interpolateId(String min, String max, int position, int total) {
        int len = min.length();
        // Convert to numeric arrays for interpolation
        int[] minArr = new int[len];
        int[] maxArr = new int[len];
        for (int i = 0; i < len; i++) {
            minArr[i] = min.charAt(i);
            maxArr[i] = max.charAt(i);
        }

        // Interpolate: result[i] = min[i] + (max[i] - min[i]) * position / total
        // Process from least significant (rightmost) to most significant for carry propagation
        char[] result = new char[len];
        long carry = 0;
        for (int i = len - 1; i >= 0; i--) {
            long diff = (long) maxArr[i] - (long) minArr[i];
            long interpolated = minArr[i] + (diff * position + carry * 128) / total;
            carry = (diff * position + carry * 128) % total;

            // Clamp to printable ASCII range
            interpolated = Math.max(minArr[i], Math.min(maxArr[i], interpolated));
            result[i] = (char) interpolated;
        }

        return new String(result);
    }

    @Override
    public String getIdFieldName() {
        return "Id";
    }

    @Override
    public void close() {
        // HttpClient doesn't need explicit close in Java 11+
    }

    // --- Internal ---

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> executeSoqlQuery(String soql) throws Exception {
        String instanceUrl = authClient.getTokenInstanceUrl();
        String token = authClient.getAccessToken();
        String encodedSoql = java.net.URLEncoder.encode(soql, java.nio.charset.StandardCharsets.UTF_8);
        String url = instanceUrl + "/services/data/" + API_VERSION + "/query/?q=" + encodedSoql;

        LOG.debug("Executing SOQL: {}", soql);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        handleErrorResponse(response);

        Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
        Object records = result.get("records");
        if (records instanceof List) {
            List<Map<String, Object>> recordList = (List<Map<String, Object>>) records;
            for (Map<String, Object> record : recordList) {
                record.remove("attributes");
            }
            return recordList;
        }

        return Collections.emptyList();
    }

    private void handleErrorResponse(HttpResponse<String> response) throws Exception {
        if (response.statusCode() == 401) {
            authClient.invalidateToken();
            throw new RuntimeException("Salesforce authentication expired, will retry on next call");
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("Salesforce API error: " + response.statusCode() + " " + response.body());
        }
    }

    private String escapeSOQL(String value) {
        return value.replace("'", "\\'");
    }
}
