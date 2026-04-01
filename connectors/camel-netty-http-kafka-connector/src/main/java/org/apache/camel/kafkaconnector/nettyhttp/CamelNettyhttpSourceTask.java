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
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.ChunkReader;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.ChunkReaderFactory;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.SnapshotCoordinator;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.SnapshotEngine;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.SnapshotMode;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.SnapshotRecord;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.SnapshotSignalConsumer;
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
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    private PayloadRouter payloadRouter;
    private SourceDlqProducer dlqProducer;
    private SnapshotEngine snapshotEngine;
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
            Set<String> fanoutFields = new HashSet<>(parseCsv(fanoutFieldsStr));
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
                dlqBootstrap = resolveBootstrapServers(props);
            }
            dlqProducer = new SourceDlqProducer(dlqBootstrap, dlqTopic);
        }

        // Snapshot only makes sense with payload routing (provider-specific API queries)
        if (payloadRouterEnabled) {
            initializeSnapshotEngine(config, props);
        }
    }

    private void initializeSnapshotEngine(CamelNettyhttpSourceConnectorConfig config, Map<String, String> props) {
        String snapshotModeStr = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_MODE_CONF);
        SnapshotMode snapshotMode;
        try {
            snapshotMode = SnapshotMode.valueOf(snapshotModeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            snapshotMode = SnapshotMode.NO_DATA;
        }

        String signalTopic = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_SIGNAL_TOPIC_CONF);
        boolean hasSignalTopic = signalTopic != null && !signalTopic.trim().isEmpty();
        boolean needsSnapshot = snapshotMode != SnapshotMode.NO_DATA || hasSignalTopic;

        if (!needsSnapshot) {
            LOG.info("Snapshot disabled (mode={}, no signal topic)", snapshotMode);
            return;
        }

        // Create chunk reader via factory — null if provider doesn't support snapshots
        String routerType = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_PAYLOAD_ROUTER_TYPE_CONF);
        ChunkReader chunkReader = ChunkReaderFactory.create(routerType, props);

        if (chunkReader == null) {
            LOG.info("Snapshot not available for provider '{}' — no ChunkReader implementation. " +
                    "Snapshot mode and signal topic will be ignored.", routerType);
            return;
        }

        int maxThreads = config.getInt(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_MAX_THREADS_CONF);
        int chunkSize = config.getInt(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_CHUNK_SIZE_CONF);
        long chunkDelayMs = config.getLong(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_CHUNK_DELAY_MS_CONF);
        boolean parallelSegments = config.getBoolean(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_PARALLEL_SEGMENTS_ENABLED_CONF);
        long parallelSegmentsMinRows = config.getLong(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_PARALLEL_SEGMENTS_MIN_ROWS_CONF);
        long signalPollIntervalMs = config.getLong(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_SIGNAL_POLL_INTERVAL_MS_CONF);

        String objectsStr = config.getString(CamelNettyhttpSourceConnectorConfig.CAMEL_SOURCE_SNAPSHOT_OBJECTS_CONF);
        List<String> initialObjects = parseCsv(objectsStr);

        String connectorName = props.getOrDefault("name", "camel-netty-http");

        SnapshotCoordinator coordinator = new SnapshotCoordinator(
                chunkReader, maxThreads, chunkSize, chunkDelayMs,
                parallelSegments, parallelSegmentsMinRows, connectorName);

        SnapshotSignalConsumer signalConsumer = null;
        if (hasSignalTopic) {
            String bootstrapServers = resolveBootstrapServers(props);
            String groupId = connectorName + "-snapshot-signals";
            signalConsumer = new SnapshotSignalConsumer(bootstrapServers, signalTopic, groupId);
        }

        snapshotEngine = new SnapshotEngine(snapshotMode, coordinator, signalConsumer,
                chunkSize, initialObjects, connectorName, signalPollIntervalMs);

        snapshotEngine.start(context);

        LOG.info("Snapshot engine initialized: mode={}, signal topic={}, objects={}, threads={}",
                snapshotMode, signalTopic, initialObjects, maxThreads);
    }


    @Override
    public synchronized List<SourceRecord> poll() {
        if (payloadRouter == null) {
            // No payload routing — use default behavior from CamelSourceTask
            return super.poll();
        }

        List<SourceRecord> records = new ArrayList<>();

        // Poll snapshot engine for signals and snapshot records
        if (snapshotEngine != null) {
            List<SnapshotRecord> snapshotRecords = snapshotEngine.poll();
            for (SnapshotRecord sr : snapshotRecords) {
                records.add(snapshotRecordToSourceRecord(sr));
            }
        }

        // Check if CDC streaming should be active
        if (snapshotEngine != null && !snapshotEngine.shouldStream()) {
            LOG.debug("CDC streaming paused during blocking snapshot");
            return records.isEmpty() ? null : records;
        }

        // CDC path: process incoming webhook exchanges
        LOG.debug("Number of records waiting an ack: {}", freeSlots.capacity() - freeSlots.size());
        final long startPollEpochMilli = Instant.now().toEpochMilli();

        long remaining = remaining(startPollEpochMilli, maxPollDuration);
        long collectedRecords = 0L;

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

    private SourceRecord snapshotRecordToSourceRecord(SnapshotRecord sr) {
        String payload;
        try {
            payload = OBJECT_MAPPER.writeValueAsString(sr.getData());
        } catch (Exception e) {
            throw new ConnectException("Failed to serialize snapshot record", e);
        }

        Schema bodySchema = SchemaHelper.buildSchemaBuilderForType(payload);

        Object recordId = sr.getRecordId();
        Schema keySchema = null;
        Object key = null;
        if (recordId != null) {
            Map<String, Object> keyFields = new java.util.LinkedHashMap<>();
            keyFields.put("Id", recordId);
            Struct keyStruct = buildKeyStruct(sr.getObjectName() + "_key", keyFields);
            keySchema = keyStruct.schema();
            key = keyStruct;
        }

        return new SourceRecord(
                sr.getSourcePartition(),
                sr.getSourceOffset(),
                sr.getObjectName(),
                null, keySchema, key,
                bodySchema, payload,
                System.currentTimeMillis());
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
        if (snapshotEngine != null) {
            snapshotEngine.shutdown();
        }
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

            Object recordKey;
            Schema recordKeySchema;
            if (routed.hasKey()) {
                Struct keyStruct = buildKeyStruct(routed.getTopic() + "_key", routed.getKeyFields());
                recordKeySchema = keyStruct.schema();
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

    private static Struct buildKeyStruct(String schemaName, Map<String, Object> keyFields) {
        SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
        for (Map.Entry<String, Object> kf : keyFields.entrySet()) {
            builder.field(kf.getKey(), SchemaHelper.buildSchemaBuilderForType(kf.getValue()));
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Map.Entry<String, Object> kf : keyFields.entrySet()) {
            struct.put(kf.getKey(), kf.getValue());
        }
        return struct;
    }

    private static String resolveBootstrapServers(Map<String, String> props) {
        // Check multiple possible bootstrap server configs in priority order
        String[] keys = {
                "camel.connector.bootstrap.servers",
                "signal.kafka.bootstrap.servers",
                "bootstrap.servers",
                "producer.bootstrap.servers"
        };
        for (String key : keys) {
            String value = props.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        return "localhost:9092";
    }

    private static List<String> parseCsv(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private void acknowledgeExchange(Exchange exchange) {
        try {
            UnitOfWorkHelper.doneSynchronizations(exchange, exchange.getExchangeExtension().handoverCompletions());
        } catch (Throwable t) {
            LOG.error("Failed to acknowledge exchange {}: {}", exchange.getExchangeId(), t.getMessage());
        }
    }
}
