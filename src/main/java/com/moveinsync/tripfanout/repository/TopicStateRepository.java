package com.moveinsync.tripfanout.repository;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.TopicSnapshot;

import java.util.List;
import java.util.Optional;

/**
 * Repository abstraction for topic state persistence, sequence allocation,
 * and sliding replay buffer management.
 */
public interface TopicStateRepository {

    /**
     * Atomically increment and return the monotonic sequence number for a topic.
     */
    long incrementSequence(String topic);

    /**
     * Retrieve the current latest sequence number for a topic (0 if none).
     */
    long getCurrentSequence(String topic);

    /**
     * Append an event to the topic's bounded replay buffer (ZSet) and evict
     * older events beyond maxBufferSize.
     */
    void appendToBuffer(String topic, Event event, long sequenceNumber, int maxBufferSize);

    /**
     * Retrieve all buffered events for a topic within a score/sequence range.
     */
    List<Event> getBufferRange(String topic, long minSeq, long maxSeq);

    /**
     * Retrieve the minimum sequence number currently available in the buffer.
     * Returns null if buffer is empty.
     */
    Long getBufferMinSequence(String topic);

    /**
     * Return the count of events currently held in the replay buffer.
     */
    long getBufferSize(String topic);

    /**
     * Persist the latest consolidated topic state snapshot.
     */
    void saveSnapshot(String topic, TopicSnapshot snapshot);

    /**
     * Retrieve the latest consolidated topic state snapshot.
     */
    Optional<TopicSnapshot> getSnapshot(String topic);

    /**
     * Delete all data for a topic (used during test teardown).
     */
    void clearTopic(String topic);
}

