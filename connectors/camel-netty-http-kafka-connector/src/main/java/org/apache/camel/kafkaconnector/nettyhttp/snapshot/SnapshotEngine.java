package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integrates snapshot functionality with the main poll() loop.
 * Handles:
 * - Initial/blocking snapshot on first run
 * - Signal-driven incremental/blocking snapshots
 * - Dedup between CDC events and snapshot windows
 * - Merging snapshot records into the SourceRecord stream
 */
public class SnapshotEngine {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotEngine.class);

    private final SnapshotMode mode;
    private final SnapshotCoordinator coordinator;
    private final SnapshotSignalConsumer signalConsumer;
    private final int defaultChunkSize;
    private final List<String> initialObjects;
    private final String connectorName;

    private boolean initialSnapshotDone = false;
    private long lastSignalPollMs = 0;
    private final long signalPollIntervalMs;

    public SnapshotEngine(SnapshotMode mode, SnapshotCoordinator coordinator,
                           SnapshotSignalConsumer signalConsumer,
                           int defaultChunkSize, List<String> initialObjects,
                           String connectorName, long signalPollIntervalMs) {
        this.mode = mode;
        this.coordinator = coordinator;
        this.signalConsumer = signalConsumer;
        this.defaultChunkSize = defaultChunkSize;
        this.initialObjects = initialObjects;
        this.connectorName = connectorName;
        this.signalPollIntervalMs = signalPollIntervalMs;
    }

    /**
     * Initialize snapshot engine. Check offsets, start initial snapshot if needed.
     */
    public void start(SourceTaskContext taskContext) {
        coordinator.start();

        if (mode.requiresInitialSnapshot()) {
            // Check stored offset to see if initial snapshot was already completed
            Map<String, String> initialPartition = new LinkedHashMap<>();
            initialPartition.put("connector", connectorName);
            initialPartition.put("snapshot_initial", "true");

            Map<String, Object> storedOffset = taskContext.offsetStorageReader().offset(initialPartition);
            if (storedOffset != null && Boolean.TRUE.equals(storedOffset.get("completed"))) {
                initialSnapshotDone = true;
                LOG.info("Initial snapshot already completed (found in offsets), skipping");
            } else {
                LOG.info("Starting initial snapshot for objects: {}", initialObjects);
                coordinator.startSnapshot(initialObjects, SnapshotType.BLOCKING,
                        "initial-" + connectorName, defaultChunkSize, null);
            }
        } else {
            // NO_DATA mode — no automatic snapshot
            initialSnapshotDone = true;
        }

        LOG.info("SnapshotEngine started, mode: {}, initial done: {}", mode, initialSnapshotDone);
    }

    /**
     * Should CDC streaming be active?
     * Returns false if blocking snapshot is in progress or mode is INITIAL_ONLY.
     */
    public boolean shouldStream() {
        if (!mode.shouldStream()) {
            return false;
        }
        // During initial blocking snapshot, pause CDC
        if (!initialSnapshotDone && coordinator.hasActiveSnapshots()) {
            return false;
        }
        return true;
    }

    /**
     * Poll for signal-triggered snapshots and drain snapshot records.
     * Called from the main poll() loop.
     */
    public List<SnapshotRecord> poll() {
        // Poll signal topic periodically
        long now = System.currentTimeMillis();
        if (signalConsumer != null && (now - lastSignalPollMs) >= signalPollIntervalMs) {
            lastSignalPollMs = now;
            processSignals();
        }

        // Check if initial snapshot completed
        if (!initialSnapshotDone && !coordinator.hasActiveSnapshots()) {
            initialSnapshotDone = true;
            LOG.info("Initial snapshot completed for all objects");
        }

        // Drain records from coordinator
        return coordinator.drainRecords(100);
    }

    /**
     * Dedup a CDC event against active snapshot windows.
     * Called from the CDC path before emitting a CDC record.
     *
     * @param objectName entity type (e.g. "Account", "ticket")
     * @param recordId   the entity's primary key
     * @return true if deduped (CDC should still be emitted, snapshot version removed)
     */
    public boolean dedupCdcEvent(String objectName, String recordId) {
        return coordinator.dedupCdcEvent(objectName, recordId);
    }

    public boolean isInitialSnapshotDone() {
        return initialSnapshotDone;
    }

    public boolean hasActiveSnapshots() {
        return coordinator.hasActiveSnapshots();
    }

    public void shutdown() {
        if (signalConsumer != null) {
            signalConsumer.close();
        }
        coordinator.shutdown();
    }

    // --- Internal ---

    private void processSignals() {
        List<SnapshotSignalConsumer.SnapshotSignal> signals = signalConsumer.poll(Duration.ofMillis(100));

        for (SnapshotSignalConsumer.SnapshotSignal signal : signals) {
            switch (signal.getType()) {
                case "execute-snapshot":
                    handleExecuteSnapshot(signal);
                    break;
                case "stop-snapshot":
                    coordinator.stopSnapshot(signal.getObjects());
                    break;
                case "pause-snapshot":
                    coordinator.pauseSnapshots(signal.getObjects());
                    break;
                case "resume-snapshot":
                    coordinator.resumeSnapshots(signal.getObjects());
                    break;
                default:
                    LOG.warn("Unknown signal type: {}", signal.getType());
            }
        }
    }

    private void handleExecuteSnapshot(SnapshotSignalConsumer.SnapshotSignal signal) {
        List<String> objects = signal.getObjects();
        if (objects.isEmpty()) {
            LOG.warn("execute-snapshot signal has no objects, ignoring");
            return;
        }

        SnapshotType type = signal.getSnapshotType();
        int chunkSize = signal.getChunkSize(defaultChunkSize);
        String additionalCondition = signal.getAdditionalCondition();

        LOG.info("Processing execute-snapshot signal: objects={}, type={}, chunkSize={}, condition={}",
                objects, type, chunkSize, additionalCondition);

        coordinator.startSnapshot(objects, type, signal.getId(), chunkSize, additionalCondition);
    }
}
