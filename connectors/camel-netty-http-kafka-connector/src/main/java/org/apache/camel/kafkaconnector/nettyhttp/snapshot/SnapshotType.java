package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

public enum SnapshotType {
    INCREMENTAL,  // Chunked, resumable, runs alongside CDC with dedup
    BLOCKING      // Full table read, pauses CDC during execution
}
