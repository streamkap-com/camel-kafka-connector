package org.apache.camel.kafkaconnector.nettyhttp.cdc;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.snapshot.salesforce.SalesforceAuthClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight Salesforce CDC subscriber using CometD/Bayeux protocol over HTTP long-polling.
 * No external CometD library needed — uses java.net.http.HttpClient directly.
 *
 * Protocol flow:
 * 1. Handshake → get clientId
 * 2. Subscribe to channels with replayId
 * 3. Connect (long-poll) → receive events, repeat
 */
public class SalesforceCdcSubscriber implements CdcSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceCdcSubscriber.class);
    private static final String API_VERSION = "59.0";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SalesforceAuthClient authClient;
    private final List<String> channels;
    private final HttpClient httpClient;

    // CometD state
    private final AtomicReference<String> clientId = new AtomicReference<>(null);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    // Replay tracking: channel → last replayId
    private final java.util.concurrent.ConcurrentHashMap<String, AtomicLong> replayIds =
            new java.util.concurrent.ConcurrentHashMap<>();

    // Event buffer: received events waiting to be consumed by poll()
    private final ConcurrentLinkedQueue<Map<String, Object>> eventQueue = new ConcurrentLinkedQueue<>();

    // Background thread for long-polling
    private Thread pollingThread;

    public SalesforceCdcSubscriber(SalesforceAuthClient authClient, List<String> channels) {
        this.authClient = authClient;
        this.channels = channels != null ? channels : Collections.emptyList();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * Initialize replay IDs from stored offsets.
     */
    public void setReplayId(String channel, long replayId) {
        replayIds.put(channel, new AtomicLong(replayId));
    }

    /**
     * Get current replay ID for a channel.
     */
    public long getReplayId(String channel) {
        AtomicLong id = replayIds.get(channel);
        return id != null ? id.get() : -1;
    }

    @Override
    public void start() {
        if (channels.isEmpty()) {
            LOG.warn("No CDC channels configured, subscriber not starting");
            return;
        }

        running.set(true);
        pollingThread = new Thread(this::pollLoop, "sf-cdc-subscriber");
        pollingThread.setDaemon(true);
        pollingThread.start();
        LOG.info("Salesforce CDC subscriber started for channels: {}", channels);
    }

    @Override
    public List<String> drainEvents(int maxEvents) {
        List<String> events = new ArrayList<>();
        while (events.size() < maxEvents) {
            Map<String, Object> event = eventQueue.poll();
            if (event == null) break;
            try {
                events.add(OBJECT_MAPPER.writeValueAsString(event));
            } catch (Exception e) {
                LOG.error("Failed to serialize CDC event: {}", e.getMessage());
            }
        }
        return events;
    }

    @Override
    public Map<String, Long> getReplayPositions() {
        Map<String, Long> positions = new java.util.LinkedHashMap<>();
        replayIds.forEach((channel, replayId) -> positions.put(channel, replayId.get()));
        return positions;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void stop() {
        running.set(false);
        if (pollingThread != null) {
            pollingThread.interrupt();
            try {
                pollingThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Salesforce CDC subscriber stopped");
    }

    // --- CometD Protocol Implementation ---

    private void pollLoop() {
        while (running.get()) {
            try {
                if (clientId.get() == null) {
                    handshake();
                    subscribe();
                }
                connect();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("CDC subscriber error: {}", e.getMessage());
                clientId.set(null);
                connected.set(false);
                // Wait before reconnecting
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        connected.set(false);
    }

    @SuppressWarnings("unchecked")
    private void handshake() throws Exception {
        String instanceUrl = authClient.getTokenInstanceUrl();
        String token = authClient.getAccessToken();
        String cometdUrl = instanceUrl + "/cometd/" + API_VERSION;

        String handshakeJson = OBJECT_MAPPER.writeValueAsString(List.of(Map.of(
                "channel", "/meta/handshake",
                "version", "1.0",
                "minimumVersion", "1.0",
                "supportedConnectionTypes", List.of("long-polling")
        )));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(cometdUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(handshakeJson))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 401 || response.statusCode() == 403) {
            authClient.invalidateToken();
            throw new RuntimeException("Auth expired during handshake, will retry");
        }

        List<Map<String, Object>> results = OBJECT_MAPPER.readValue(response.body(), List.class);
        if (results.isEmpty()) {
            throw new RuntimeException("Empty handshake response");
        }

        Map<String, Object> handshakeResponse = results.get(0);
        Boolean successful = (Boolean) handshakeResponse.get("successful");
        if (!Boolean.TRUE.equals(successful)) {
            throw new RuntimeException("Handshake failed: " + handshakeResponse);
        }

        clientId.set((String) handshakeResponse.get("clientId"));
        LOG.info("CometD handshake successful, clientId: {}", clientId.get());
    }

    @SuppressWarnings("unchecked")
    private void subscribe() throws Exception {
        String instanceUrl = authClient.getTokenInstanceUrl();
        String token = authClient.getAccessToken();
        String cometdUrl = instanceUrl + "/cometd/" + API_VERSION;

        for (String channel : channels) {
            long replayId = getReplayId(channel);

            Map<String, Object> subscribeMsg = new java.util.LinkedHashMap<>();
            subscribeMsg.put("channel", "/meta/subscribe");
            subscribeMsg.put("clientId", clientId.get());
            subscribeMsg.put("subscription", channel);

            // Replay extension: -1 = all new, -2 = all available (up to 72h)
            Map<String, Object> ext = new java.util.LinkedHashMap<>();
            Map<String, Object> replay = new java.util.LinkedHashMap<>();
            replay.put(channel, replayId > 0 ? replayId : -1);
            ext.put("replay", replay);
            subscribeMsg.put("ext", ext);

            String subscribeJson = OBJECT_MAPPER.writeValueAsString(List.of(subscribeMsg));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(cometdUrl))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(subscribeJson))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            List<Map<String, Object>> results = OBJECT_MAPPER.readValue(response.body(), List.class);
            if (!results.isEmpty()) {
                Map<String, Object> subResponse = results.get(0);
                Boolean successful = (Boolean) subResponse.get("successful");
                if (Boolean.TRUE.equals(successful)) {
                    LOG.info("Subscribed to CDC channel: {} (replayFrom: {})", channel, replayId);
                } else {
                    LOG.error("Subscribe failed for {}: {}", channel, subResponse);
                    throw new RuntimeException("Subscribe failed for " + channel + ": " + subResponse);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void connect() throws Exception {
        String instanceUrl = authClient.getTokenInstanceUrl();
        String token = authClient.getAccessToken();
        String cometdUrl = instanceUrl + "/cometd/" + API_VERSION;

        String connectJson = OBJECT_MAPPER.writeValueAsString(List.of(Map.of(
                "channel", "/meta/connect",
                "clientId", clientId.get(),
                "connectionType", "long-polling"
        )));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(cometdUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectJson))
                .timeout(Duration.ofSeconds(120)) // Long-poll timeout
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 401 || response.statusCode() == 403) {
            authClient.invalidateToken();
            clientId.set(null);
            throw new RuntimeException("Auth expired during connect, will reconnect");
        }

        List<Map<String, Object>> messages = OBJECT_MAPPER.readValue(response.body(), List.class);
        connected.set(true);

        for (Map<String, Object> message : messages) {
            String channel = (String) message.get("channel");

            // Skip meta messages
            if (channel != null && channel.startsWith("/meta/")) {
                Boolean successful = (Boolean) message.get("successful");
                if (!Boolean.TRUE.equals(successful)) {
                    LOG.warn("CometD meta message not successful: {}", message);
                    // Reconnect on failed connect
                    if ("/meta/connect".equals(channel)) {
                        clientId.set(null);
                    }
                }
                continue;
            }

            // This is a CDC data event
            if (channel != null && message.containsKey("data")) {
                Map<String, Object> data = (Map<String, Object>) message.get("data");

                // Track replayId
                if (data.containsKey("event")) {
                    Map<String, Object> event = (Map<String, Object>) data.get("event");
                    if (event != null && event.containsKey("replayId")) {
                        long newReplayId = ((Number) event.get("replayId")).longValue();
                        replayIds.computeIfAbsent(channel, k -> new AtomicLong(-1)).set(newReplayId);
                    }
                }

                // Build the full event payload matching our expected format
                Map<String, Object> fullEvent = new java.util.LinkedHashMap<>();
                fullEvent.put("data", data);
                fullEvent.put("channel", channel);

                eventQueue.add(fullEvent);

                LOG.debug("CDC event received on {}: {}", channel,
                        data.containsKey("payload") ? ((Map<?, ?>) data.get("payload")).get("ChangeEventHeader") : "unknown");
            }
        }
    }
}
