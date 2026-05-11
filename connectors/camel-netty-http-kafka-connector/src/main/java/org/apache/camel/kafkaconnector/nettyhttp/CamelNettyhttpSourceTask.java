/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.kafkaconnector.nettyhttp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.StreamCache;
import org.apache.camel.kafkaconnector.CamelSourceConnectorConfig;
import org.apache.camel.kafkaconnector.CamelSourceRecord;
import org.apache.camel.kafkaconnector.CamelSourceTask;
import org.apache.camel.kafkaconnector.nettyhttp.dlq.SourceDlqProducer;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRouter;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.apache.camel.kafkaconnector.utils.SchemaHelper;
import org.apache.camel.kafkaconnector.utils.TaskHelper;
import org.apache.camel.support.UnitOfWorkHelper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamelNettyhttpSourceTask extends CamelSourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(CamelNettyhttpSourceTask.class);

    private PayloadRouter payloadRouter;
    private SourceDlqProducer dlqProducer;
    private final ConcurrentHashMap<String, AtomicInteger> exchangeRefCounts = new ConcurrentHashMap<>();

    @Override
    protected CamelSourceConnectorConfig getCamelSourceConnectorConfig(
            Map<String, String> props) {
        return new CamelNettyhttpSourceConnectorConfig(props);
    }

    @Override
    protected Map<String, String> getDefaultConfig() {
        return new HashMap<String, String>() {{
            put(CamelSourceConnectorConfig.CAMEL_SOURCE_COMPONENT_CONF, "netty-http");
        }};
    }

    @Override
    public void start(Map<String, String> props) {
        super.start(props);

        // Initialize payload router if enabled
        CamelNettyhttpSourceConnectorConfig config = new CamelNettyhttpSourceConnectorConfig(props);
        boolean payloadRouterEnabled = config.getBoolean(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_ENABLED_CONF);

        if (payloadRouterEnabled) {
            String routerType = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_TYPE_CONF);
            String topicPrefix = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_TOPIC_PREFIX_CONF);
            String unknownBehaviorStr = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_UNKNOWN_BEHAVIOR_CONF);
            String defaultTopic = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_DEFAULT_TOPIC_CONF);

            UnknownTypeBehavior unknownBehavior;
            try {
                unknownBehavior = UnknownTypeBehavior.valueOf(unknownBehaviorStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ConnectException("Invalid value for " + CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_UNKNOWN_BEHAVIOR_CONF
                        + ": " + unknownBehaviorStr + ". Valid values: DEFAULT_TOPIC, SKIP, FAIL");
            }

            // Advanced routing config
            String fanoutFieldsStr = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_FANOUT_FIELDS_CONF);
            Set<String> fanoutFields = (fanoutFieldsStr == null || fanoutFieldsStr.trim().isEmpty())
                    ? Collections.emptySet()
                    : Arrays.stream(fanoutFieldsStr.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toSet());
            boolean flattenDetail = config.getBoolean(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_FLATTEN_DETAIL_CONF);
            String flattenDetailPrefix = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_FLATTEN_DETAIL_PREFIX_CONF);
            boolean includeEvent = config.getBoolean(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_INCLUDE_EVENT_CONF);

            PayloadRoutingStrategy strategy = PayloadRouter.createStrategy(routerType);
            strategy.configure(topicPrefix, unknownBehavior, defaultTopic);
            strategy.configureAdvanced(fanoutFields, flattenDetail, flattenDetailPrefix, includeEvent);
            payloadRouter = new PayloadRouter(strategy);
            LOG.info("Payload routing enabled with type '{}', topic prefix '{}', fanout fields: {}, flatten detail: {}",
                    routerType, topicPrefix, fanoutFields, flattenDetail);
        }

        // Initialize DLQ producer if enabled
        boolean dlqEnabled = config.getBoolean(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_DLQ_ENABLED_CONF);
        if (dlqEnabled) {
            String dlqTopic = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_DLQ_TOPIC_CONF);
            String dlqBootstrap = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_DLQ_BOOTSTRAP_SERVERS_CONF);
            if (dlqBootstrap == null || dlqBootstrap.trim().isEmpty()) {
                // Fall back to worker's bootstrap servers
                dlqBootstrap = props.getOrDefault("camel.connector.bootstrap.servers",
                        props.getOrDefault("bootstrap.servers", "localhost:9092"));
            }
            dlqProducer = new SourceDlqProducer(dlqBootstrap, dlqTopic);
        }
    }

    @Override
    public synchronized List<SourceRecord> poll() {
        if (payloadRouter == null) {
            // No payload routing — use default behavior from CamelSourceTask
            return super.poll();
        }

        // Payload routing mode
        LOG.debug("Number of records waiting an ack: {}", freeSlots.capacity() - freeSlots.size());
        final long startPollEpochMilli = Instant.now().toEpochMilli();

        long remaining = remaining(startPollEpochMilli, maxPollDuration);
        long collectedRecords = 0L;

        List<SourceRecord> records = new ArrayList<>();
        while (collectedRecords < maxBatchPollSize && freeSlots.size() >= 1 && remaining > 0) {
            Exchange exchange = consumer.receive(remaining);
            if (exchange == null) {
                break;
            }

            LOG.debug("Received Exchange {} with Message {} from Endpoint {}", exchange.getExchangeId(),
                    exchange.getMessage().getMessageId(), exchange.getFromEndpoint());

            // Extract body as string before any processing (preserved for DLQ)
            String bodyString = extractBodyString(exchange);

            try {
                processExchange(exchange, bodyString, records);
            } catch (PayloadRoutingException e) {
                LOG.error("Payload routing failed for exchange {}: {}", exchange.getExchangeId(), e.getMessage());
                sendToDlq(bodyString, null, null, e);
                acknowledgeExchange(exchange);
                continue;
            } catch (Exception e) {
                LOG.error("Unexpected error processing exchange {}: {}", exchange.getExchangeId(), e.getMessage(), e);
                sendToDlq(bodyString, null, null, e);
                acknowledgeExchange(exchange);
                continue;
            }

            collectedRecords++;
            remaining = remaining(startPollEpochMilli, maxPollDuration);
        }

        return records.isEmpty() ? null : records;
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (payloadRouter == null) {
            super.commitRecord(record, metadata);
            return;
        }

        // Fan-out commit with reference counting
        LOG.debug("Committing record: {} with metadata: {}", record, metadata);
        CamelSourceRecord camelRecord = (CamelSourceRecord) record;
        Integer claimCheck = camelRecord.getClaimCheck();
        LOG.debug("Committing record with claim check number: {}", claimCheck);
        Exchange correlatedExchange = exchangesWaitingForAck[claimCheck];

        try {
            String sourceExchangeId = camelRecord.getSourceExchangeId();
            if (sourceExchangeId != null) {
                AtomicInteger refCount = exchangeRefCounts.get(sourceExchangeId);
                if (refCount != null && refCount.decrementAndGet() == 0) {
                    UnitOfWorkHelper.doneSynchronizations(correlatedExchange, correlatedExchange.getExchangeExtension().handoverCompletions());
                    exchangeRefCounts.remove(sourceExchangeId);
                    LOG.debug("All fan-out records committed for exchange: {}", sourceExchangeId);
                }
            } else {
                UnitOfWorkHelper.doneSynchronizations(correlatedExchange, correlatedExchange.getExchangeExtension().handoverCompletions());
            }
            LOG.debug("Record with claim check number: {} committed.", claimCheck);
        } catch (Throwable t) {
            LOG.error("Exception during Unit Of Work completion: {} caused by: {}", t.getMessage(), t.getCause());
            throw new RuntimeException(t);
        } finally {
            exchangesWaitingForAck[claimCheck] = null;
            freeSlots.add(claimCheck);
            LOG.debug("Claim check number: {} freed.", claimCheck);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (dlqProducer != null) {
            dlqProducer.close();
        }
    }

    private void processExchange(Exchange exchange, String bodyString, List<SourceRecord> records) {
        Map<String, String> sourcePartition = Collections.singletonMap("filename", exchange.getFromEndpoint().toString());
        Map<String, String> sourceOffset = Collections.singletonMap("position", exchange.getExchangeId());

        final Object messageHeaderKey = camelMessageHeaderKey != null ? exchange.getMessage().getHeader(camelMessageHeaderKey) : null;
        final Schema messageKeySchema = messageHeaderKey != null ? SchemaHelper.buildSchemaBuilderForType(messageHeaderKey) : null;
        final long timestamp = calculateTimestamp(exchange);

        List<RoutedRecord> routedRecords = payloadRouter.route(bodyString);

        if (routedRecords.isEmpty()) {
            acknowledgeExchange(exchange);
            return;
        }

        // Check if we have enough free slots
        if (freeSlots.size() < routedRecords.size()) {
            LOG.debug("Insufficient free slots ({}) for routed records ({}), will retry next poll",
                    freeSlots.size(), routedRecords.size());
            return;
        }

        // Track reference count for fan-out
        String exchangeId = exchange.getExchangeId();
        exchangeRefCounts.put(exchangeId, new AtomicInteger(routedRecords.size()));

        for (RoutedRecord routed : routedRecords) {
            Schema bodySchema = SchemaHelper.buildSchemaBuilderForType(routed.getPayload());

            // Build struct key from routed record's key fields, or fall back to header-based key
            Object recordKey;
            Schema recordKeySchema;
            if (routed.hasKey()) {
                Map<String, Object> keyFields = routed.getKeyFields();
                SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(routed.getTopic() + "_key");
                for (Map.Entry<String, Object> kf : keyFields.entrySet()) {
                    keySchemaBuilder.field(kf.getKey(), SchemaHelper.buildSchemaBuilderForType(kf.getValue()));
                }
                recordKeySchema = keySchemaBuilder.build();
                Struct keyStruct = new Struct(recordKeySchema);
                for (Map.Entry<String, Object> kf : keyFields.entrySet()) {
                    keyStruct.put(kf.getKey(), kf.getValue());
                }
                recordKey = keyStruct;
            } else {
                recordKey = messageHeaderKey;
                recordKeySchema = recordKey != null ? SchemaHelper.buildSchemaBuilderForType(recordKey) : null;
            }

            CamelSourceRecord camelRecord = new CamelSourceRecord(sourcePartition, sourceOffset,
                    routed.getTopic(), null, recordKeySchema, recordKey,
                    bodySchema, routed.getPayload(), timestamp);

            camelRecord.setEventType(routed.getEventType());
            camelRecord.setSourceExchangeId(exchangeId);

            if (mapHeaders && exchange.getMessage().hasHeaders()) {
                setAdditionalHeaders(camelRecord, exchange.getMessage().getHeaders(), HEADER_CAMEL_PREFIX);
            }
            if (mapProperties && exchange.hasProperties()) {
                setAdditionalHeaders(camelRecord, exchange.getProperties(), PROPERTY_CAMEL_PREFIX);
            }

            TaskHelper.logRecordContent(LOG, loggingLevel, camelRecord);
            Integer claimCheck = freeSlots.remove();
            camelRecord.setClaimCheck(claimCheck);
            exchangesWaitingForAck[claimCheck] = exchange;
            LOG.debug("Routed record to topic: {}, claim check: {}", routed.getTopic(), claimCheck);
            records.add(camelRecord);
        }
    }

    private void sendToDlq(String rawPayload, String intendedTopic, String eventType, Exception error) {
        if (dlqProducer == null) {
            LOG.warn("DLQ not enabled. Dropping errored record. Error: {} Payload: {}",
                    error.getMessage(), rawPayload);
            return;
        }
        dlqProducer.send(rawPayload, intendedTopic, eventType,
                error.getClass().getName(), error.getMessage());
    }

    private String extractBodyString(Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        if (body instanceof String) {
            return (String) body;
        }
        if (body instanceof StreamCache) {
            ((StreamCache) body).reset();
        }
        return exchange.getMessage().getBody(String.class);
    }

    private void acknowledgeExchange(Exchange exchange) {
        try {
            UnitOfWorkHelper.doneSynchronizations(exchange, exchange.getExchangeExtension().handoverCompletions());
        } catch (Throwable t) {
            LOG.error("Failed to acknowledge exchange {}: {}", exchange.getExchangeId(), t.getMessage());
        }
    }
}
