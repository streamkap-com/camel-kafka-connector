package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotContextTest {

    private SnapshotSegment createSegment(String objectName) {
        return SnapshotSegment.singleSegment(objectName, "test-corr", SnapshotType.INCREMENTAL, null);
    }

    // --- Lifecycle ---

    @Test
    void testLifecycle() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));

        assertEquals(SnapshotContext.Status.PENDING, ctx.getStatus());
        assertFalse(ctx.isRunning());

        ctx.start();
        assertEquals(SnapshotContext.Status.IN_PROGRESS, ctx.getStatus());
        assertTrue(ctx.isRunning());

        ctx.complete();
        assertEquals(SnapshotContext.Status.COMPLETED, ctx.getStatus());
        assertFalse(ctx.isRunning());
        assertTrue(ctx.isCompleted());
    }

    @Test
    void testPauseResume() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.start();

        ctx.pause();
        assertTrue(ctx.isPaused());
        assertEquals(SnapshotContext.Status.PAUSED, ctx.getStatus());

        ctx.resume();
        assertFalse(ctx.isPaused());
        assertTrue(ctx.isRunning());
    }

    // --- Dedup Window ---

    @Test
    void testWindowDedupBasic() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.start();

        // Open window and add records
        ctx.openWindow();
        assertTrue(ctx.isWindowOpen());

        ctx.addToWindow("001", Map.of("Name", "Acme"));
        ctx.addToWindow("002", Map.of("Name", "Globex"));
        ctx.addToWindow("003", Map.of("Name", "Initech"));

        // CDC event for record 001 — should dedup
        assertTrue(ctx.dedup("001"));
        // CDC event for record 999 — not in window
        assertFalse(ctx.dedup("999"));

        // Close window — remaining records
        Map<String, Map<String, Object>> remaining = ctx.closeWindow();
        assertEquals(2, remaining.size());
        assertNotNull(remaining.get("002"));
        assertNotNull(remaining.get("003"));
        assertNull(remaining.get("001")); // Was deduped
        assertFalse(ctx.isWindowOpen());
    }

    @Test
    void testDedupWhenWindowClosed() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.start();

        // Window not open — dedup should return false
        assertFalse(ctx.dedup("001"));
    }

    @Test
    void testWindowClearsOnOpen() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));

        ctx.openWindow();
        ctx.addToWindow("001", Map.of("Name", "Old"));
        ctx.closeWindow();

        // Reopen window — should be empty
        ctx.openWindow();
        assertFalse(ctx.dedup("001"));
    }

    // --- Chunk Position ---

    @Test
    void testChunkAdvancement() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));

        assertNull(ctx.getLastKey());
        assertEquals(0, ctx.getChunkNumber());

        ctx.advanceChunk("001D000000Abc");
        assertEquals("001D000000Abc", ctx.getLastKey());
        assertEquals(1, ctx.getChunkNumber());

        ctx.advanceChunk("001D000000Xyz");
        assertEquals("001D000000Xyz", ctx.getLastKey());
        assertEquals(2, ctx.getChunkNumber());
    }

    @Test
    void testEffectiveStartKey() {
        SnapshotSegment segment = new SnapshotSegment("Account", 0, "001_START", "001_END",
                "test", SnapshotType.INCREMENTAL, null);
        SnapshotContext ctx = new SnapshotContext(segment);

        // Before any chunk — use segment start key
        assertEquals("001_START", ctx.getEffectiveStartKey());

        // After advancing — use last key
        ctx.advanceChunk("001_MIDDLE");
        assertEquals("001_MIDDLE", ctx.getEffectiveStartKey());
    }

    // --- Offset Serialization ---

    @Test
    void testOffsetRoundTrip() {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.start();
        ctx.setMaxKey("001_MAX");
        ctx.advanceChunk("001_CURRENT");
        ctx.advanceChunk("001_NEXT");

        Map<String, Object> offset = ctx.toOffset();

        assertEquals("IN_PROGRESS", offset.get("status"));
        assertEquals("001_NEXT", offset.get("last_key"));
        assertEquals("001_MAX", offset.get("max_key"));
        assertEquals(2, offset.get("chunk_number"));
        assertEquals("INCREMENTAL", offset.get("snapshot_type"));

        // Restore into new context
        SnapshotContext restored = new SnapshotContext(createSegment("Account"));
        restored.restoreFromOffset(offset);

        assertEquals("001_NEXT", restored.getLastKey());
        assertEquals("001_MAX", restored.getMaxKey());
        assertEquals(2, restored.getChunkNumber());
        assertTrue(restored.isRunning());
    }

    @Test
    void testRestoreCompletedOffset() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("status", "COMPLETED");

        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.restoreFromOffset(offset);

        assertTrue(ctx.isCompleted());
    }

    // --- Thread Safety ---

    @Test
    void testConcurrentDedup() throws Exception {
        SnapshotContext ctx = new SnapshotContext(createSegment("Account"));
        ctx.start();
        ctx.openWindow();

        // Add 1000 records
        for (int i = 0; i < 1000; i++) {
            ctx.addToWindow("id_" + i, Map.of("index", i));
        }

        // Dedup 500 records concurrently from multiple threads
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(500);
        int[] dedupCount = {0};

        for (int i = 0; i < 500; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    if (ctx.dedup("id_" + idx)) {
                        synchronized (dedupCount) {
                            dedupCount[0]++;
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // All 500 should have been deduped
        assertEquals(500, dedupCount[0]);

        // Close window — should have 500 remaining
        Map<String, Map<String, Object>> remaining = ctx.closeWindow();
        assertEquals(500, remaining.size());
    }

    // --- Segment ---

    @Test
    void testSegmentSourcePartition() {
        SnapshotSegment segment = new SnapshotSegment("Account", 2, "A", "B",
                "corr-1", SnapshotType.INCREMENTAL, null);

        Map<String, String> partition = segment.toSourcePartition("my-connector");
        assertEquals("my-connector", partition.get("connector"));
        assertEquals("Account", partition.get("snapshot_object"));
        assertEquals("2", partition.get("segment"));
    }

    @Test
    void testSegmentId() {
        SnapshotSegment segment = new SnapshotSegment("Contact", 3, null, null,
                "corr-1", SnapshotType.BLOCKING, null);
        assertEquals("Contact_segment_3", segment.getSegmentId());
    }

    // --- Snapshot Mode ---

    @Test
    void testSnapshotModeInitial() {
        assertTrue(SnapshotMode.INITIAL.requiresInitialSnapshot());
        assertTrue(SnapshotMode.INITIAL.shouldStream());
    }

    @Test
    void testSnapshotModeInitialOnly() {
        assertTrue(SnapshotMode.INITIAL_ONLY.requiresInitialSnapshot());
        assertFalse(SnapshotMode.INITIAL_ONLY.shouldStream());
    }

    @Test
    void testSnapshotModeNoData() {
        assertFalse(SnapshotMode.NO_DATA.requiresInitialSnapshot());
        assertTrue(SnapshotMode.NO_DATA.shouldStream());
    }
}
