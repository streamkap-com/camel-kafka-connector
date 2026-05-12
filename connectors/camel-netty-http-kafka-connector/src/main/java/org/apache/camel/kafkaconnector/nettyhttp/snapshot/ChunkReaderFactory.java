package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.Map;

import org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce.SalesforceAuthClient;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce.SalesforceChunkReader;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.shopify.ShopifyAuthClient;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.shopify.ShopifyChunkReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating provider-specific ChunkReader instances.
 * Each provider encapsulates its own authentication and API logic.
 */
public class ChunkReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkReaderFactory.class);

    /**
     * Create a ChunkReader for the given provider type.
     *
     * @param providerType  provider name (e.g. "salesforce", "zendesk")
     * @param config        full connector config map for provider-specific settings
     * @return ChunkReader instance, or null if the provider doesn't support snapshots
     */
    public static ChunkReader create(String providerType, Map<String, String> config) {
        if (providerType == null) {
            return null;
        }

        switch (providerType.toLowerCase()) {
            case "salesforce":
                return createSalesforceReader(config);
            case "shopify":
                return createShopifyReader(config);
            case "zendesk":
                // Zendesk chunk reader not yet implemented
                LOG.warn("Zendesk snapshot ChunkReader not yet implemented. Signal-triggered snapshots will not work.");
                return null;
            default:
                LOG.warn("No snapshot ChunkReader available for provider '{}'. Signal-triggered snapshots will not work.", providerType);
                return null;
        }
    }

    private static ChunkReader createSalesforceReader(Map<String, String> config) {
        String instanceUrl = config.getOrDefault("camel.source.snapshot.salesforce.instance.url", "");
        String clientId = config.getOrDefault("camel.source.snapshot.salesforce.auth.client.id", "");
        String clientSecret = config.getOrDefault("camel.source.snapshot.salesforce.auth.client.secret", "");
        String username = config.getOrDefault("camel.source.snapshot.salesforce.auth.username", "");
        String password = config.getOrDefault("camel.source.snapshot.salesforce.auth.password", "");

        if (instanceUrl.isEmpty() || clientId.isEmpty() || username.isEmpty()) {
            LOG.warn("Salesforce snapshot API credentials not configured. Snapshot will not work.");
            return null;
        }

        SalesforceAuthClient authClient = new SalesforceAuthClient(
                instanceUrl, clientId, clientSecret, username, password);
        return new SalesforceChunkReader(authClient);
    }

    private static ChunkReader createShopifyReader(Map<String, String> config) {
        String storeUrl = config.getOrDefault("camel.source.snapshot.shopify.store.url", "");
        String accessToken = config.getOrDefault("camel.source.snapshot.shopify.access.token", "");
        String apiVersion = config.getOrDefault("camel.source.snapshot.shopify.api.version", "2024-10");

        if (storeUrl.isEmpty() || accessToken.isEmpty()) {
            LOG.warn("Shopify snapshot credentials not configured (store URL or access token missing). Snapshot will not work.");
            return null;
        }

        ShopifyAuthClient authClient = new ShopifyAuthClient(storeUrl, accessToken, apiVersion);
        return new ShopifyChunkReader(authClient);
    }
}
