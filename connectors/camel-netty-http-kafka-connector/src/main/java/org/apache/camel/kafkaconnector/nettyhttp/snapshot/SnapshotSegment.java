package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;

public class SnapshotSegment {
    private final String objectName;
    private final int segmentIndex;
    private final String startKey;      // inclusive, null = beginning
    private final String endKey;        // exclusive, null = end of table
    private final String correlationId;
    private final SnapshotType snapshotType;
    private final String additionalCondition;

    public SnapshotSegment(String objectName, int segmentIndex, String startKey, String endKey,
                           String correlationId, SnapshotType snapshotType, String additionalCondition) {
        this.objectName = objectName;
        this.segmentIndex = segmentIndex;
        this.startKey = startKey;
        this.endKey = endKey;
        this.correlationId = correlationId;
        this.snapshotType = snapshotType;
        this.additionalCondition = additionalCondition;
    }

    public static SnapshotSegment singleSegment(String objectName, String correlationId,
                                                 SnapshotType snapshotType, String additionalCondition) {
        return new SnapshotSegment(objectName, 0, null, null, correlationId, snapshotType, additionalCondition);
    }

    public String getObjectName() {
        return objectName;
    }

    public int getSegmentIndex() {
        return segmentIndex;
    }

    public String getStartKey() {
        return startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public SnapshotType getSnapshotType() {
        return snapshotType;
    }

    public String getAdditionalCondition() {
        return additionalCondition;
    }

    public Map<String, String> toSourcePartition(String connectorName) {
        Map<String, String> partition = new LinkedHashMap<>();
        partition.put("connector", connectorName);
        partition.put("snapshot_object", objectName);
        partition.put("segment", String.valueOf(segmentIndex));
        return partition;
    }

    public String getSegmentId() {
        return objectName + "_segment_" + segmentIndex;
    }

    @Override
    public String toString() {
        return "SnapshotSegment{object='" + objectName + "', segment=" + segmentIndex
                + ", keys=[" + startKey + ", " + endKey + ")}";
    }
}
