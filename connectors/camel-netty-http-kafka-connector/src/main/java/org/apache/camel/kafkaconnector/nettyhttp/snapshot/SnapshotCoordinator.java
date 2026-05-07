package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages parallel snapshot execution. Handles:
 * - Thread pool for concurrent object/segment snapshots
 * - Segment splitting for large tables
 * - Offset recovery on restart
 * - Output record buffering (thread-safe queue)
 */
public class SnapshotCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotCoordinator.class);

    private final ChunkReader chunkReader;
    private final int maxThreads;
    private final int chunkSize;
    private final long chunkDelayMs;
    private final boolean parallelSegmentsEnabled;
    private final long parallelSegmentsMinRows;
    private final String connectorName;

    private ExecutorService executorService;

    // Active contexts: segmentId → context (thread-safe for CDC dedup lookups)
    private final ConcurrentHashMap<String, SnapshotContext> activeContexts = new ConcurrentHashMap<>();
    // Object name → list of contexts (for CDC dedup by object)
    private final ConcurrentHashMap<String, List<SnapshotContext>> contextsByObject = new ConcurrentHashMap<>();

    private static final int OUTPUT_QUEUE_CAPACITY = 10000;

    // Output buffer: snapshot records ready to be consumed by poll(). Bounded to prevent OOM.
    private final LinkedBlockingQueue<SnapshotRecord> outputQueue = new LinkedBlockingQueue<>(OUTPUT_QUEUE_CAPACITY);

    public SnapshotCoordinator(ChunkReader chunkReader, int maxThreads, int chunkSize,
                                long chunkDelayMs, boolean parallelSegmentsEnabled,
                                long parallelSegmentsMinRows, String connectorName) {
        this.chunkReader = chunkReader;
        this.maxThreads = maxThreads;
        this.chunkSize = chunkSize;
        this.chunkDelayMs = chunkDelayMs;
        this.parallelSegmentsEnabled = parallelSegmentsEnabled;
        this.parallelSegmentsMinRows = parallelSegmentsMinRows;
        this.connectorName = connectorName;
    }

    public void start() {
        this.executorService = Executors.newFixedThreadPool(maxThreads);
        LOG.info("SnapshotCoordinator started with {} threads, chunk size {}", maxThreads, chunkSize);
    }

    /**
     * Start a snapshot for multiple objects, potentially in parallel.
     * For blocking snapshots, each object may be further split into segments.
     */
    public void startSnapshot(List<String> objects, SnapshotType type, String correlationId,
                               int chunkSizeOverride, String additionalCondition) {
        int effectiveChunkSize = chunkSizeOverride > 0 ? chunkSizeOverride : chunkSize;
        String corrId = correlationId != null ? correlationId : UUID.randomUUID().toString();

        for (String objectName : objects) {
            List<SnapshotSegment> segments = createSegments(objectName, type, corrId,
                    effectiveChunkSize, additionalCondition);

            for (SnapshotSegment segment : segments) {
                SnapshotContext context = new SnapshotContext(segment);
                registerContext(context);

                executorService.submit(() -> runSegmentSnapshot(context, effectiveChunkSize));
            }
        }
    }

    /**
     * Resume in-progress snapshots from stored offsets (called on restart).
     */
    public void resumeSnapshots(Map<SnapshotSegment, Map<String, Object>> storedOffsets) {
        for (Map.Entry<SnapshotSegment, Map<String, Object>> entry : storedOffsets.entrySet()) {
            SnapshotSegment segment = entry.getKey();
            Map<String, Object> offset = entry.getValue();

            String status = (String) offset.get("status");
            if (SnapshotContext.Status.COMPLETED.name().equals(status)) {
                continue;
            }

            SnapshotContext context = new SnapshotContext(segment);
            context.restoreFromOffset(offset);
            registerContext(context);

            int storedChunkSize = chunkSize; // Could be stored in offset if needed
            executorService.submit(() -> runSegmentSnapshot(context, storedChunkSize));

            LOG.info("Resumed snapshot for {} from key: {}", segment, context.getLastKey());
        }
    }

    /**
     * Stop snapshot for specific objects, or all if objects list is empty.
     */
    public void stopSnapshot(List<String> objects) {
        if (objects == null || objects.isEmpty()) {
            // Stop all
            LOG.info("Stopping ALL active snapshots");
            for (SnapshotContext ctx : activeContexts.values()) {
                ctx.fail("Stopped by signal");
            }
            activeContexts.clear();
            contextsByObject.clear();
            return;
        }

        for (String objectName : objects) {
            List<SnapshotContext> contexts = contextsByObject.get(objectName);
            if (contexts != null) {
                LOG.info("Stopping snapshot for object: {}", objectName);
                for (SnapshotContext ctx : contexts) {
                    ctx.fail("Stopped by signal");
                    activeContexts.remove(ctx.getSegment().getSegmentId());
                }
                contextsByObject.remove(objectName);
            }
        }
    }

    /**
     * Pause snapshots for specific objects, or all if objects list is empty.
     */
    public void pauseSnapshots(List<String> objects) {
        if (objects == null || objects.isEmpty()) {
            LOG.info("Pausing ALL active snapshots");
            activeContexts.values().forEach(SnapshotContext::pause);
            return;
        }

        for (String objectName : objects) {
            List<SnapshotContext> contexts = contextsByObject.get(objectName);
            if (contexts != null) {
                LOG.info("Pausing snapshot for object: {}", objectName);
                contexts.forEach(SnapshotContext::pause);
            }
        }
    }

    /**
     * Resume snapshots for specific objects, or all if objects list is empty.
     */
    public void resumeSnapshots(List<String> objects) {
        if (objects == null || objects.isEmpty()) {
            LOG.info("Resuming ALL paused snapshots");
            activeContexts.values().forEach(ctx -> {
                if (ctx.isPaused()) ctx.resume();
            });
            return;
        }

        for (String objectName : objects) {
            List<SnapshotContext> contexts = contextsByObject.get(objectName);
            if (contexts != null) {
                LOG.info("Resuming snapshot for object: {}", objectName);
                contexts.forEach(ctx -> {
                    if (ctx.isPaused()) ctx.resume();
                });
            }
        }
    }

    /**
     * Try to dedup a CDC event against any open snapshot window for the given object.
     * Called from the CDC path in poll().
     *
     * @return true if the record was deduped (removed from a snapshot window)
     */
    public boolean dedupCdcEvent(String objectName, String recordId) {
        List<SnapshotContext> contexts = contextsByObject.get(objectName);
        if (contexts == null) {
            return false;
        }
        for (SnapshotContext ctx : contexts) {
            if (ctx.dedup(recordId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Drain snapshot records from the output queue.
     * Called by poll() to merge snapshot records with CDC records.
     */
    public List<SnapshotRecord> drainRecords(int maxRecords) {
        List<SnapshotRecord> records = new ArrayList<>();
        while (records.size() < maxRecords) {
            SnapshotRecord record = outputQueue.poll();
            if (record == null) {
                break;
            }
            records.add(record);
        }
        return records;
    }

    public boolean hasActiveSnapshots() {
        return activeContexts.values().stream().anyMatch(c -> c.isRunning() || c.isPaused());
    }

    public boolean hasRecords() {
        return !outputQueue.isEmpty();
    }

    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (chunkReader != null) {
            chunkReader.close();
        }
        LOG.info("SnapshotCoordinator shut down");
    }

    // --- Internal ---

    private List<SnapshotSegment> createSegments(String objectName, SnapshotType type,
                                                   String correlationId, int chunkSize,
                                                   String additionalCondition) {
        if (!parallelSegmentsEnabled || maxThreads <= 1) {
            return Collections.singletonList(
                    SnapshotSegment.singleSegment(objectName, correlationId, type, additionalCondition));
        }

        try {
            long count = chunkReader.getApproximateCount(objectName, additionalCondition);
            if (count < parallelSegmentsMinRows) {
                LOG.info("{} has {} rows (< {}), using single segment",
                        objectName, count, parallelSegmentsMinRows);
                return Collections.singletonList(
                        SnapshotSegment.singleSegment(objectName, correlationId, type, additionalCondition));
            }

            int numSegments = Math.min(maxThreads, (int) Math.ceil((double) count / chunkSize));
            numSegments = Math.max(2, numSegments); // At least 2 if splitting

            List<String> boundaries = chunkReader.getSegmentBoundaries(objectName, numSegments, additionalCondition);

            List<SnapshotSegment> segments = new ArrayList<>();
            String prevBoundary = null;
            for (int i = 0; i <= boundaries.size(); i++) {
                String endBoundary = i < boundaries.size() ? boundaries.get(i) : null;
                segments.add(new SnapshotSegment(objectName, i, prevBoundary, endBoundary,
                        correlationId, type, additionalCondition));
                prevBoundary = endBoundary;
            }

            LOG.info("Split {} into {} parallel segments (count: {})", objectName, segments.size(), count);
            return segments;

        } catch (Exception e) {
            LOG.warn("Failed to split {} into segments, using single segment: {}", objectName, e.getMessage());
            return Collections.singletonList(
                    SnapshotSegment.singleSegment(objectName, correlationId, type, additionalCondition));
        }
    }

    private void registerContext(SnapshotContext context) {
        String segmentId = context.getSegment().getSegmentId();
        String objectName = context.getSegment().getObjectName();

        activeContexts.put(segmentId, context);
        contextsByObject.computeIfAbsent(objectName, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(context);
    }

    private void runSegmentSnapshot(SnapshotContext context, int chunkSize) {
        SnapshotSegment segment = context.getSegment();
        String segmentId = segment.getSegmentId();

        try {
            context.start();

            // Determine max key if not already set (first run)
            if (context.getMaxKey() == null) {
                String maxKey = chunkReader.getMaxKey(segment.getObjectName(), segment.getAdditionalCondition());
                context.setMaxKey(maxKey);
            }

            while (context.isRunning()) {
                // Check for pause
                while (context.isPaused()) {
                    Thread.sleep(1000);
                }

                String afterKey = context.getEffectiveStartKey();
                String endKey = segment.getEndKey();

                // Open dedup window
                context.openWindow();

                // Read chunk
                List<Map<String, Object>> chunk = chunkReader.readChunk(
                        segment.getObjectName(), afterKey, endKey, chunkSize,
                        segment.getAdditionalCondition());

                if (chunk.isEmpty()) {
                    context.closeWindow();
                    context.complete();
                    break;
                }

                // Add chunk records to window for dedup
                String lastKeyInChunk = null;
                for (Map<String, Object> record : chunk) {
                    String recordId = String.valueOf(record.get(chunkReader.getIdFieldName()));
                    context.addToWindow(recordId, record);
                    lastKeyInChunk = recordId;
                }

                // Small delay to allow CDC events to arrive for dedup
                Thread.sleep(Math.min(chunkDelayMs, 200));

                // Close window — remaining records (not deduped by CDC) go to output
                Map<String, Map<String, Object>> remaining = context.closeWindow();

                for (Map.Entry<String, Map<String, Object>> entry : remaining.entrySet()) {
                    Map<String, String> sourcePartition = segment.toSourcePartition(connectorName);
                    Map<String, Object> sourceOffset = context.toOffset();

                    // Build key fields using the ChunkReader's id field name
                    String idFieldName = chunkReader.getIdFieldName();
                    Map<String, Object> keyFields = new java.util.LinkedHashMap<>();
                    keyFields.put(idFieldName, entry.getKey());

                    Map<String, Object> recordData = entry.getValue();
                    recordData.put("__changeType", "SNAPSHOT");
                    recordData.put("__deleted", false);

                    SnapshotRecord snapshotRecord = new SnapshotRecord(
                            segment.getObjectName(), recordData, keyFields,
                            sourcePartition, sourceOffset);
                    outputQueue.put(snapshotRecord);
                }

                // Advance position
                context.advanceChunk(lastKeyInChunk);

                LOG.debug("Snapshot {} chunk {} done, {} records emitted, last key: {}",
                        segmentId, context.getChunkNumber(), remaining.size(), lastKeyInChunk);

                // Delay between chunks
                if (chunkDelayMs > 0) {
                    Thread.sleep(chunkDelayMs);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.fail("Interrupted");
        } catch (Exception e) {
            LOG.error("Snapshot failed for {}: {}", segmentId, e.getMessage(), e);
            context.fail(e.getMessage());
        } finally {
            // Cleanup
            activeContexts.remove(segmentId);
            List<SnapshotContext> objectContexts = contextsByObject.get(segment.getObjectName());
            if (objectContexts != null) {
                objectContexts.remove(context);
                if (objectContexts.isEmpty()) {
                    contextsByObject.remove(segment.getObjectName());
                }
            }

            LOG.info("Snapshot segment finished: {} status: {}", segmentId, context.getStatus());
        }
    }
}
