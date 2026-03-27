package org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce;

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
 * OAuth2 client for Salesforce API authentication.
 * Handles token acquisition and refresh using username-password flow.
 */
public class SalesforceAuthClient {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceAuthClient.class);

    private final String instanceUrl;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private String accessToken;
    private String tokenInstanceUrl;
    private long tokenExpiresAt;

    public SalesforceAuthClient(String instanceUrl, String clientId, String clientSecret,
                                 String username, String password) {
        this.instanceUrl = instanceUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    @SuppressWarnings("unchecked")
    public synchronized String getAccessToken() throws Exception {
        if (accessToken != null && System.currentTimeMillis() < tokenExpiresAt) {
            return accessToken;
        }

        String loginUrl = instanceUrl + "/services/oauth2/token";
        String body = "grant_type=password"
                + "&client_id=" + encode(clientId)
                + "&client_secret=" + encode(clientSecret)
                + "&username=" + encode(username)
                + "&password=" + encode(password);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Salesforce OAuth2 failed: " + response.statusCode() + " " + response.body());
        }

        Map<String, Object> tokenResponse = objectMapper.readValue(response.body(), Map.class);
        accessToken = (String) tokenResponse.get("access_token");
        tokenInstanceUrl = (String) tokenResponse.get("instance_url");
        // Salesforce tokens don't have explicit expiry — refresh every 90 minutes
        tokenExpiresAt = System.currentTimeMillis() + (90 * 60 * 1000);

        LOG.info("Salesforce OAuth2 token acquired for instance: {}", tokenInstanceUrl);
        return accessToken;
    }

    public String getTokenInstanceUrl() throws Exception {
        getAccessToken(); // Ensure token is valid
        return tokenInstanceUrl != null ? tokenInstanceUrl : instanceUrl;
    }

    public void invalidateToken() {
        accessToken = null;
        tokenExpiresAt = 0;
    }

    private String encode(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
