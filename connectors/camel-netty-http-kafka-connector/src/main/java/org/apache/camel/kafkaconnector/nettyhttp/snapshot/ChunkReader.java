package org.apache.camel.kafkaconnector.nettyhttp.snapshot;

import java.util.List;
import java.util.Map;

/**
 * Interface for reading chunks of data from a source API.
 * Implementations are provider-specific (Salesforce, Zendesk, etc.)
 */
public interface ChunkReader {

    /**
     * Read a chunk of records from the source.
     *
     * @param objectName    the object/entity to read (e.g. "Account", "tickets")
     * @param afterKey      read records with Id > afterKey (null = start from beginning)
     * @param endKey        read records with Id < endKey (null = no upper bound)
     * @param chunkSize     max records to return
     * @param additionalCondition  optional WHERE clause filter (null = no filter)
     * @return list of records as maps, ordered by Id ascending
     */
    List<Map<String, Object>> readChunk(String objectName, String afterKey, String endKey,
                                         int chunkSize, String additionalCondition) throws Exception;

    /**
     * Get the maximum Id value for an object. Used for progress estimation
     * and segment boundary calculation.
     */
    String getMaxKey(String objectName, String additionalCondition) throws Exception;

    /**
     * Get approximate row count for an object. Used to decide whether
     * to split into parallel segments.
     */
    long getApproximateCount(String objectName, String additionalCondition) throws Exception;

    /**
     * Get boundary keys for splitting an object into parallel segments.
     * Returns N-1 boundary keys that divide the object into N segments.
     *
     * @param objectName    the object to split
     * @param numSegments   number of segments desired
     * @param additionalCondition  optional filter
     * @return list of boundary keys (size = numSegments - 1)
     */
    List<String> getSegmentBoundaries(String objectName, int numSegments,
                                       String additionalCondition) throws Exception;

    /**
     * Get the Id field name for the object's primary key.
     */
    String getIdFieldName();

    void close();
}
