package org.apache.camel.kafkaconnector.nettyhttp.snapshot.shopify;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auth client for Shopify Admin API.
 *
 * Supports two modes:
 * 1. Static access token (legacy custom apps) — permanent, never expires
 * 2. Client credentials grant (Dev Dashboard apps) — tokens expire every 24 hours,
 *    auto-refreshed using client ID + secret
 *
 * If clientId and clientSecret are provided, uses client credentials grant.
 * Otherwise, uses the static access token.
 */
public class ShopifyAuthClient {

    private static final Logger LOG = LoggerFactory.getLogger(ShopifyAuthClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String storeUrl;
    private final String apiVersion;
    private final HttpClient httpClient;

    // Static token mode
    private final String staticAccessToken;

    // Client credentials mode
    private final String clientId;
    private final String clientSecret;

    // Token state (for client credentials mode)
    private String accessToken;
    private long tokenExpiresAt;

    /**
     * Create auth client with static access token (legacy custom app).
     */
    public ShopifyAuthClient(String storeUrl, String accessToken, String apiVersion) {
        this(storeUrl, accessToken, apiVersion, null, null);
    }

    /**
     * Create auth client with optional client credentials grant support.
     * If clientId and clientSecret are provided, uses OAuth2 client credentials.
     * Otherwise falls back to static access token.
     */
    public ShopifyAuthClient(String storeUrl, String accessToken, String apiVersion,
                              String clientId, String clientSecret) {
        this.storeUrl = normalizeUrl(storeUrl);
        this.apiVersion = apiVersion != null && !apiVersion.isEmpty() ? apiVersion : "2024-10";
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();

        this.clientId = clientId;
        this.clientSecret = clientSecret;

        if (clientId != null && !clientId.isEmpty()
                && clientSecret != null && !clientSecret.isEmpty()) {
            // Client credentials mode — token will be fetched on first use
            this.staticAccessToken = null;
            this.accessToken = null;
            this.tokenExpiresAt = 0;
            LOG.info("Shopify auth configured with client credentials grant (tokens expire every 24h)");
        } else {
            // Static token mode
            this.staticAccessToken = accessToken;
            this.accessToken = accessToken;
            this.tokenExpiresAt = Long.MAX_VALUE;
            LOG.info("Shopify auth configured with static access token");
        }
    }

    public String getStoreUrl() {
        return storeUrl;
    }

    @SuppressWarnings("unchecked")
    public synchronized String getAccessToken() throws Exception {
        if (accessToken != null && System.currentTimeMillis() < tokenExpiresAt) {
            return accessToken;
        }

        if (staticAccessToken != null) {
            // Static mode — token doesn't expire
            accessToken = staticAccessToken;
            return accessToken;
        }

        // Client credentials grant
        LOG.info("Requesting new Shopify access token via client credentials grant");

        String tokenUrl = storeUrl + "/admin/oauth/access_token";
        String body = OBJECT_MAPPER.writeValueAsString(Map.of(
                "client_id", clientId,
                "client_secret", clientSecret,
                "grant_type", "client_credentials"
        ));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Shopify OAuth2 client credentials grant failed: "
                    + response.statusCode() + " " + response.body());
        }

        Map<String, Object> tokenResponse = OBJECT_MAPPER.readValue(response.body(), Map.class);
        accessToken = (String) tokenResponse.get("access_token");

        if (accessToken == null || accessToken.isEmpty()) {
            throw new RuntimeException("Shopify OAuth2 response missing access_token: " + response.body());
        }

        // Tokens are valid for 24 hours — refresh at 23 hours to avoid edge-case expiry
        tokenExpiresAt = System.currentTimeMillis() + (23 * 60 * 60 * 1000);

        LOG.info("Shopify access token acquired, expires in ~23 hours");
        return accessToken;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public String getGraphqlEndpoint() {
        return storeUrl + "/admin/api/" + apiVersion + "/graphql.json";
    }

    public void invalidateToken() {
        accessToken = null;
        tokenExpiresAt = 0;
    }

    /**
     * Execute a GraphQL query against the Shopify Admin API.
     * Retries on timeout (up to 2 retries) and auto-refreshes token on 401.
     */
    public HttpResponse<String> executeGraphql(String queryBody) throws Exception {
        int maxRetries = 2;
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                HttpRequest request = buildGraphqlRequest(queryBody);
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                // Auto-retry on 401 with fresh token
                if (response.statusCode() == 401 && staticAccessToken == null) {
                    LOG.info("Shopify token expired, refreshing...");
                    invalidateToken();
                    request = buildGraphqlRequest(queryBody);
                    response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                }

                if (response.statusCode() == 401 || response.statusCode() == 403) {
                    throw new RuntimeException("Shopify authentication failed (" + response.statusCode()
                            + "). Check your credentials.");
                }
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Shopify API error: " + response.statusCode() + " " + response.body());
                }
                return response;

            } catch (java.net.http.HttpTimeoutException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long waitMs = (attempt + 1) * 5000L;
                    LOG.warn("Shopify GraphQL request timed out (attempt {}/{}), retrying in {}ms...",
                            attempt + 1, maxRetries + 1, waitMs);
                    Thread.sleep(waitMs);
                }
            }
        }
        throw new RuntimeException("Shopify GraphQL request timed out after " + (maxRetries + 1) + " attempts", lastException);
    }

    private HttpRequest buildGraphqlRequest(String queryBody) throws Exception {
        return HttpRequest.newBuilder()
                .uri(URI.create(getGraphqlEndpoint()))
                .header("Content-Type", "application/json")
                .header("X-Shopify-Access-Token", getAccessToken())
                .POST(HttpRequest.BodyPublishers.ofString(queryBody))
                .timeout(Duration.ofSeconds(60))
                .build();
    }

    private static String normalizeUrl(String url) {
        String normalized = url.replaceAll("/+$", "");
        if (!normalized.startsWith("https://") && !normalized.startsWith("http://")) {
            normalized = "https://" + normalized;
        }
        return normalized;
    }
}
