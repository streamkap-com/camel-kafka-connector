package org.apache.camel.kafkaconnector.nettyhttp.routing;

import java.util.List;
import java.util.Map;

public interface PayloadRoutingStrategy {

    void configure(String topicPrefix, UnknownTypeBehavior unknownTypeBehavior, String defaultTopic);

    List<RoutedRecord> route(Map<String, Object> payload);
}
