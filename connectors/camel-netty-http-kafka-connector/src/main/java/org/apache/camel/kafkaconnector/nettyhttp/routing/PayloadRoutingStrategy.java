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
}