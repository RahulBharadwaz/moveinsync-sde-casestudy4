package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.TopicSnapshot;
import com.moveinsync.tripfanout.repository.RedisTopicStateRepository;
import com.moveinsync.tripfanout.repository.TopicStateRepository;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Core authority for strictly ordered sequence assignment, bounded replay buffer persistence,
 * state snapshot materialization, and cross-node broadcast.
 * Follows clean Service-Repository layering.
 */
@Slf4j
@Service
public class EventSequencingService {

    private final TopicStateRepository repository;
    private final EventPublisher eventPublisher;

    @Getter
    @Setter
    private int maxBufferSize;

    public EventSequencingService(
            TopicStateRepository repository,
            EventPublisher eventPublisher,
            @Value("${app.replay-buffer.max-size:100}") int maxBufferSize) {
        this.repository = repository;
        this.eventPublisher = eventPublisher;
        this.maxBufferSize = maxBufferSize;
    }

    public static String getSequenceKey(String topic) {
        return RedisTopicStateRepository.getSequenceKey(topic);
    }

    public static String getBufferKey(String topic) {
        return RedisTopicStateRepository.getBufferKey(topic);
    }

    public static String getSnapshotKey(String topic) {
        return RedisTopicStateRepository.getSnapshotKey(topic);
    }

    /**
     * Ingest an event:
     * 1. Assign strict monotonically increasing sequence number for its topic via repository.
     * 2. Store in bounded replay buffer (ZSET) and evict events older than maxBufferSize.
     * 3. Update the aggregated topic snapshot.
     * 4. Broadcast event to Redis Pub/Sub for cross-node gateway fanout.
     */
    public Event ingest(Event event) {
        if (event == null || event.getTopic() == null || event.getTopic().isBlank()) {
            throw new IllegalArgumentException("Event and valid topic are required for ingestion");
        }

        String topic = event.getTopic();

        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }

        // 1. Strict atomic sequence allocation per topic via repository
        long sequenceNumber = repository.incrementSequence(topic);
        event.setSequenceNumber(sequenceNumber);

        try {
            // 2. Add to bounded replay buffer (ZSET: score = sequenceNumber)
            repository.appendToBuffer(topic, event, sequenceNumber, maxBufferSize);

            // 3. Update current topic state snapshot
            updateSnapshot(topic, event);

            // 4. Broadcast across cluster nodes via Redis Pub/Sub
            eventPublisher.publish(event);

            log.debug("Ingested and published event [{}] on topic [{}] with seq [{}]",
                    event.getEventId(), topic, sequenceNumber);

            return event;
        } catch (Exception e) {
            log.error("Failed to process event [{}] for topic [{}]", event.getEventId(), topic, e);
            throw new RuntimeException("Event ingestion failed for topic " + topic, e);
        }
    }

    /**
     * Update the latest aggregated state snapshot for this topic.
     */
    private synchronized void updateSnapshot(String topic, Event event) {
        TopicSnapshot snapshot = repository.getSnapshot(topic)
                .orElseGet(() -> TopicSnapshot.builder().topic(topic).build());

        if (event.getSequenceNumber() >= snapshot.getLastSequenceNumber()) {
            snapshot.applyEvent(event);
            repository.saveSnapshot(topic, snapshot);
        }
    }

    /**
     * Returns the current latest sequence number for a topic (0 if none).
     */
    public long getCurrentSequenceNumber(String topic) {
        return repository.getCurrentSequence(topic);
    }

    /**
     * Returns the current number of events stored in the replay buffer for a topic.
     */
    public long getBufferSize(String topic) {
        return repository.getBufferSize(topic);
    }
}
