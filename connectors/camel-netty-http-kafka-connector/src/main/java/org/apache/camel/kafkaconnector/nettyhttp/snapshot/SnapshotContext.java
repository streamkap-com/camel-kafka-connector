package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe per-segment snapshot state. Each parallel segment gets its own context.
 * Manages: dedup window, chunk position, completion status, offset tracking.
 */
public class SnapshotContext {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotContext.class);

    public enum Status {
        PENDING, IN_PROGRESS, COMPLETED, FAILED, PAUSED
    }

    private final SnapshotSegment segment;

    // Thread-safe state
    private final AtomicReference<Status> status = new AtomicReference<>(Status.PENDING);
    private final AtomicReference<String> lastKey = new AtomicReference<>(null);
    private final AtomicReference<String> maxKey = new AtomicReference<>(null);
    private final AtomicInteger chunkNumber = new AtomicInteger(0);
    private final AtomicBoolean windowOpen = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    // Dedup window: recordId → snapshot data (thread-safe for CDC dedup lookups)
    private final ConcurrentHashMap<String, Map<String, Object>> window = new ConcurrentHashMap<>();

    public SnapshotContext(SnapshotSegment segment) {
        this.segment = segment;
    }

    // --- Lifecycle ---

    public void start() {
        status.set(Status.IN_PROGRESS);
        LOG.info("Snapshot started: {}", segment);
    }

    public void complete() {
        status.set(Status.COMPLETED);
        windowOpen.set(false);
        window.clear();
        LOG.info("Snapshot completed: {}", segment);
    }

    public void fail(String reason) {
        status.set(Status.FAILED);
        windowOpen.set(false);
        window.clear();
        LOG.error("Snapshot failed: {} reason: {}", segment, reason);
    }

    public void pause() {
        paused.set(true);
        status.set(Status.PAUSED);
    }

    public void resume() {
        paused.set(false);
        status.set(Status.IN_PROGRESS);
    }

    public boolean isPaused() {
        return paused.get();
    }

    public boolean isRunning() {
        return status.get() == Status.IN_PROGRESS;
    }

    public boolean isCompleted() {
        return status.get() == Status.COMPLETED;
    }

    public Status getStatus() {
        return status.get();
    }

    // --- Window Management (thread-safe for CDC dedup) ---

    public void openWindow() {
        window.clear();
        windowOpen.set(true);
    }

    public void addToWindow(String recordId, Map<String, Object> data) {
        window.put(recordId, data);
    }

    /**
     * Try to dedup a CDC event against this window.
     * If the record is in the window, remove it (CDC version wins).
     *
     * @return true if the record was in the window and removed (CDC should be emitted)
     */
    public boolean dedup(String recordId) {
        if (!windowOpen.get()) {
            return false;
        }
        Map<String, Object> removed = window.remove(recordId);
        if (removed != null) {
            LOG.debug("Dedup: CDC event for {} removed from snapshot window of {}", recordId, segment.getSegmentId());
            return true;
        }
        return false;
    }

    /**
     * Close window and return remaining records (not seen in CDC).
     * These should be emitted as snapshot records.
     */
    public Map<String, Map<String, Object>> closeWindow() {
        windowOpen.set(false);
        Map<String, Map<String, Object>> remaining = new LinkedHashMap<>(window);
        window.clear();
        return remaining;
    }

    public boolean isWindowOpen() {
        return windowOpen.get();
    }

    // --- Chunk Position ---

    public void advanceChunk(String newLastKey) {
        lastKey.set(newLastKey);
        chunkNumber.incrementAndGet();
    }

    public String getLastKey() {
        return lastKey.get();
    }

    /**
     * Get the effective start key for the next chunk.
     * Uses lastKey if available (resume), otherwise segment's startKey.
     */
    public String getEffectiveStartKey() {
        String lk = lastKey.get();
        return lk != null ? lk : segment.getStartKey();
    }

    public void setMaxKey(String key) {
        maxKey.set(key);
    }

    public String getMaxKey() {
        return maxKey.get();
    }

    public int getChunkNumber() {
        return chunkNumber.get();
    }

    public SnapshotSegment getSegment() {
        return segment;
    }

    // --- Offset Serialization ---

    public Map<String, Object> toOffset() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("status", status.get().name());
        offset.put("last_key", lastKey.get());
        offset.put("max_key", maxKey.get());
        offset.put("chunk_number", chunkNumber.get());
        offset.put("correlation_id", segment.getCorrelationId());
        offset.put("snapshot_type", segment.getSnapshotType().name());
        offset.put("segment_end_key", segment.getEndKey());
        return offset;
    }

    /**
     * Restore context from stored offset.
     */
    public void restoreFromOffset(Map<String, Object> offset) {
        if (offset == null) {
            return;
        }
        String storedStatus = (String) offset.get("status");
        if (Status.COMPLETED.name().equals(storedStatus)) {
            status.set(Status.COMPLETED);
            return;
        }
        if (Status.IN_PROGRESS.name().equals(storedStatus) || Status.PAUSED.name().equals(storedStatus)) {
            status.set(Status.IN_PROGRESS);
        }
        if (offset.get("last_key") != null) {
            lastKey.set(String.valueOf(offset.get("last_key")));
        }
        if (offset.get("max_key") != null) {
            maxKey.set(String.valueOf(offset.get("max_key")));
        }
        if (offset.get("chunk_number") instanceof Number) {
            chunkNumber.set(((Number) offset.get("chunk_number")).intValue());
        }
    }
}
