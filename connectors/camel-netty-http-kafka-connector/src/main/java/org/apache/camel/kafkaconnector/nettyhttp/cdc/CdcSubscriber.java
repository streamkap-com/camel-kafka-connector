package org.apache.camel.kafkaconnector.nettyhttp.cdc;

import java.util.List;
import java.util.Map;

/**
 * Interface for provider-specific CDC subscribers.
 * Implementations handle connecting to the source system's change stream
 * and delivering events in a format compatible with the PayloadRouter.
 */
public interface CdcSubscriber {

    void start();

    /**
     * Drain received events as raw JSON strings ready for PayloadRouter.
     */
    List<String> drainEvents(int maxEvents);

    /**
     * Get the last replay/offset position for a channel.
     * Used for offset tracking.
     */
    Map<String, Long> getReplayPositions();

    boolean isRunning();

    void stop();
}
