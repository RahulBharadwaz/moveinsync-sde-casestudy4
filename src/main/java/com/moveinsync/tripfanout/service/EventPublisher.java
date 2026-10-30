package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.model.Event;

/**
 * Strategy interface for broadcasting events across distributed nodes.
 */
public interface EventPublisher {

    /**
     * Broadcast an event to subscribers across cluster gateway nodes.
     *
     * @param event Sequenced and buffered event to publish
     */
    void publish(Event event);

    /**
     * Resolves the Redis pub/sub channel for a specific topic.
     *
     * @param topic Topic identifier (e.g. "cab:CAB-101", "trip:TRIP-500", "office:BLR-01")
     * @return Channel name
     */
    String getChannelForTopic(String topic);
}

