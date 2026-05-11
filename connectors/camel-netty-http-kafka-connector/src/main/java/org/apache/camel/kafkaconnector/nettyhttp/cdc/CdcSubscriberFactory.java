package org.apache.camel.kafkaconnector.nettyhttp.cdc;

import java.util.List;
import java.util.Map;

import org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce.SalesforceAuthClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating provider-specific CDC subscribers.
 */
public class CdcSubscriberFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CdcSubscriberFactory.class);

    /**
     * Create a CDC subscriber for the given provider type.
     *
     * @param providerType provider name (e.g. "salesforce")
     * @param channels     list of channels/topics to subscribe to
     * @param config       full connector config for provider-specific settings
     * @return CdcSubscriber instance, or null if provider doesn't support CDC
     */
    public static CdcSubscriber create(String providerType, List<String> channels, Map<String, String> config) {
        if (providerType == null) {
            return null;
        }

        switch (providerType.toLowerCase()) {
            case "salesforce":
                return createSalesforceSubscriber(channels, config);
            case "zendesk":
                LOG.warn("Zendesk CDC subscription not supported. Use webhook-based CDC instead.");
                return null;
            default:
                LOG.warn("No CDC subscriber available for provider '{}'. Use webhook-based CDC instead.", providerType);
                return null;
        }
    }

    private static CdcSubscriber createSalesforceSubscriber(List<String> channels, Map<String, String> config) {
        String instanceUrl = config.getOrDefault("camel.source.snapshot.salesforce.instance.url", "");
        String clientId = config.getOrDefault("camel.source.snapshot.salesforce.auth.client.id", "");
        String clientSecret = config.getOrDefault("camel.source.snapshot.salesforce.auth.client.secret", "");
        String username = config.getOrDefault("camel.source.snapshot.salesforce.auth.username", "");
        String password = config.getOrDefault("camel.source.snapshot.salesforce.auth.password", "");

        if (instanceUrl.isEmpty() || clientId.isEmpty()) {
            LOG.warn("Salesforce CDC enabled but credentials not configured. Set camel.source.snapshot.salesforce.* properties.");
            return null;
        }

        SalesforceAuthClient authClient = new SalesforceAuthClient(instanceUrl, clientId, clientSecret, username, password);
        return new SalesforceCdcSubscriber(authClient, channels);
    }
}
