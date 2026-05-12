package org.apache.camel.kafkaconnector.nettyhttp.snapshot.shopify;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auth client for Shopify Admin API.
 * Uses a static access token (from a custom/private app) rather than OAuth2 flow.
 * Shopify custom apps use permanent access tokens that don't expire.
 */
public class ShopifyAuthClient {

    private static final Logger LOG = LoggerFactory.getLogger(ShopifyAuthClient.class);

    private final String storeUrl;
    private final String accessToken;
    private final String apiVersion;
    private final HttpClient httpClient;

    public ShopifyAuthClient(String storeUrl, String accessToken, String apiVersion) {
        this.storeUrl = normalizeUrl(storeUrl);
        this.accessToken = accessToken;
        this.apiVersion = apiVersion != null && !apiVersion.isEmpty() ? apiVersion : "2024-10";
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public String getStoreUrl() {
        return storeUrl;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public String getGraphqlEndpoint() {
        return storeUrl + "/admin/api/" + apiVersion + "/graphql.json";
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * Execute a GraphQL query against the Shopify Admin API.
     */
    public HttpResponse<String> executeGraphql(String queryBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getGraphqlEndpoint()))
                .header("Content-Type", "application/json")
                .header("X-Shopify-Access-Token", accessToken)
                .POST(HttpRequest.BodyPublishers.ofString(queryBody))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 401 || response.statusCode() == 403) {
            throw new RuntimeException("Shopify authentication failed (" + response.statusCode()
                    + "). Check your access token.");
        }
        if (response.statusCode() != 200) {
            throw new RuntimeException("Shopify API error: " + response.statusCode() + " " + response.body());
        }

        return response;
    }

    private static String normalizeUrl(String url) {
        String normalized = url.replaceAll("/+$", "");
        if (!normalized.startsWith("https://") && !normalized.startsWith("http://")) {
            normalized = "https://" + normalized;
        }
        return normalized;
    }
}
