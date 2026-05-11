package org.apache.camel.kafkaconnector.nettyhttp.routing;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.salesforce.SalesforcePayloadStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk.ZendeskPayloadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PayloadRouter {

    private static final Logger LOG = LoggerFactory.getLogger(PayloadRouter.class);

    private final PayloadRoutingStrategy strategy;
    private final ObjectMapper objectMapper;

    public PayloadRouter(PayloadRoutingStrategy strategy) {
        this.strategy = strategy;
        this.objectMapper = new ObjectMapper();
    }

    @SuppressWarnings("unchecked")
    public List<RoutedRecord> route(String jsonBody) {
        if (jsonBody == null || jsonBody.trim().isEmpty()) {
            LOG.warn("Received empty or null payload, skipping");
            return Collections.emptyList();
        }

        Map<String, Object> payload;
        try {
            payload = objectMapper.readValue(jsonBody, Map.class);
        } catch (Exception e) {
            LOG.error("Failed to parse JSON payload: {}", e.getMessage());
            throw new PayloadRoutingException("Invalid JSON payload", e);
        }

        List<RoutedRecord> records = strategy.route(payload);
        LOG.debug("Payload routed to {} record(s)", records.size());
        return records;
    }

    public static PayloadRoutingStrategy createStrategy(String type) {
        if (type == null || type.trim().isEmpty()) {
            throw new PayloadRoutingException("Payload router type cannot be empty");
        }

        switch (type.toLowerCase()) {
            case "zendesk":
                return new ZendeskPayloadStrategy();
            case "salesforce":
                return new SalesforcePayloadStrategy();
            default:
                throw new PayloadRoutingException("Unknown payload router type: '" + type
                        + "'. Supported types: zendesk, salesforce");
        }
    }
}
