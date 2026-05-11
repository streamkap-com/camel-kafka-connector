package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class SnapshotRecord {
    private final String objectName;
    private final Map<String, Object> data;
    private final Map<String, Object> keyFields;
    private final Map<String, String> sourcePartition;
    private final Map<String, Object> sourceOffset;

    public SnapshotRecord(String objectName, Map<String, Object> data, Map<String, Object> keyFields,
                          Map<String, String> sourcePartition, Map<String, Object> sourceOffset) {
        this.objectName = objectName;
        this.data = data;
        this.keyFields = keyFields != null ? keyFields : Collections.emptyMap();
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

    public String getObjectName() {
        return objectName;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, Object> getKeyFields() {
        return keyFields;
    }

    public boolean hasKey() {
        return !keyFields.isEmpty();
    }

    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    public Map<String, Object> getSourceOffset() {
        return sourceOffset;
    }
}
