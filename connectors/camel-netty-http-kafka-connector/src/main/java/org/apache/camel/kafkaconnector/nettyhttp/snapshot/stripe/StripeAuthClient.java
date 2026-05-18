package org.apache.camel.kafkaconnector.nettyhttp.snapshot.stripe;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auth client for Stripe API.
 * Uses a static API secret key (sk_live_xxx or sk_test_xxx).
 * Stripe API keys don't expire, so no refresh logic needed.
 */
public class StripeAuthClient {

    private static final Logger LOG = LoggerFactory.getLogger(StripeAuthClient.class);
    private static final String BASE_URL = "https://api.stripe.com";

    private final String apiKey;
    private final HttpClient httpClient;

    public StripeAuthClient(String apiKey) {
        this.apiKey = apiKey;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public String getApiKey() {
        return apiKey;
    }

    /**
     * Execute a GET request against the Stripe API.
     * Retries on timeout (up to 2 retries with backoff).
     */
    public HttpResponse<String> executeGet(String path) throws Exception {
        String url = BASE_URL + path;
        int maxRetries = 2;
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", "Bearer " + apiKey)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .GET()
                        .timeout(Duration.ofSeconds(60))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 401) {
                    throw new RuntimeException("Stripe authentication failed. Check your API key.");
                }
                if (response.statusCode() == 429) {
                    // Rate limited — wait and retry
                    long waitMs = (attempt + 1) * 2000L;
                    LOG.warn("Stripe rate limited (429), retrying in {}ms...", waitMs);
                    Thread.sleep(waitMs);
                    continue;
                }
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Stripe API error: " + response.statusCode() + " " + response.body());
                }
                return response;

            } catch (java.net.http.HttpTimeoutException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long waitMs = (attempt + 1) * 5000L;
                    LOG.warn("Stripe API request timed out (attempt {}/{}), retrying in {}ms...",
                            attempt + 1, maxRetries + 1, waitMs);
                    Thread.sleep(waitMs);
                }
            }
        }
        throw new RuntimeException("Stripe API request timed out after " + (maxRetries + 1) + " attempts", lastException);
    }
}
