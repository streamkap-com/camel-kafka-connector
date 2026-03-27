package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.Map;

public class SnapshotRecord {
    private final String objectName;
    private final String recordId;
    private final Map<String, Object> data;
    private final Map<String, String> sourcePartition;
    private final Map<String, Object> sourceOffset;

    public SnapshotRecord(String objectName, String recordId, Map<String, Object> data,
                          Map<String, String> sourcePartition, Map<String, Object> sourceOffset) {
        this.objectName = objectName;
        this.recordId = recordId;
        this.data = data;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getRecordId() {
        return recordId;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    public Map<String, Object> getSourceOffset() {
        return sourceOffset;
    }
}
