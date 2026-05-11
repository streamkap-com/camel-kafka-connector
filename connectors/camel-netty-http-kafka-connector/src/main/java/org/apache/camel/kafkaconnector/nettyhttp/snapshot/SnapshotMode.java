package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

public enum SnapshotMode {
    INITIAL,        // Blocking snapshot on first run, then CDC streaming
    INITIAL_ONLY,   // Blocking snapshot on first run, then stop (no CDC)
    NO_DATA;        // CDC streaming only, no automatic snapshot

    public boolean requiresInitialSnapshot() {
        return this == INITIAL || this == INITIAL_ONLY;
    }

    public boolean shouldStream() {
        return this != INITIAL_ONLY;
    }
}
