package org.apache.camel.kafkaconnector.nettyhttp.routing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PayloadRoutingStrategy {

    void configure(String topicPrefix, UnknownTypeBehavior unknownTypeBehavior, String defaultTopic);

    default void configureAdvanced(Set<String> fanoutFields, boolean flattenDetail,
                                   String flattenDetailPrefix, boolean includeEvent) {
        configureAdvanced(fanoutFields, flattenDetail, flattenDetailPrefix, includeEvent, null);
    }

    default void configureAdvanced(Set<String> fanoutFields, boolean flattenDetail,
                                   String flattenDetailPrefix, boolean includeEvent,
                                   Set<String> allowedObjects) {
        // Default no-op for backward compatibility
    }

    List<RoutedRecord> route(Map<String, Object> payload);

    /**
     * Route a payload with access to HTTP headers.
     * Providers like Shopify that carry event metadata in headers override this method.
     * Default delegates to body-only route() for backward compatibility.
     */
    default List<RoutedRecord> route(Map<String, Object> payload, Map<String, Object> headers) {
        return route(payload);
    }

    /**
     * Configure HMAC signature verification (e.g., Shopify webhook signing).
     * Default no-op for providers that don't use HMAC.
     */
    default void configureHmac(String hmacSecret) {
        // no-op
    }
}