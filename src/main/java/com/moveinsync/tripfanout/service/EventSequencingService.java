package com.moveinsync.tripfanout.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.TopicSnapshot;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Core authority for strictly ordered sequence assignment, bounded replay buffer persistence,
 * state snapshot materialization, and cross-node broadcast.
 */
@Slf4j
@Service
public class EventSequencingService {

    public static final String SEQ_KEY_PREFIX = "topic:%s:seq";
    public static final String BUFFER_KEY_PREFIX = "topic:%s:buffer";
    public static final String SNAPSHOT_KEY_PREFIX = "topic:%s:snapshot";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final EventPublisher eventPublisher;

    @Getter
    @Setter
    private int maxBufferSize;

    public EventSequencingService(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            EventPublisher eventPublisher,
            @Value("${app.replay-buffer.max-size:100}") int maxBufferSize) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.maxBufferSize = maxBufferSize;
    }

    public static String getSequenceKey(String topic) {
        return String.format(SEQ_KEY_PREFIX, topic);
    }

    public static String getBufferKey(String topic) {
        return String.format(BUFFER_KEY_PREFIX, topic);
    }

    public static String getSnapshotKey(String topic) {
        return String.format(SNAPSHOT_KEY_PREFIX, topic);
    }

    /**
     * Ingest an event:
     * 1. Assign strict monotonically increasing sequence number for its topic.
     * 2. Store in bounded replay buffer (ZSET) and evict events older than maxBufferSize.
     * 3. Update the aggregated topic snapshot.
     * 4. Broadcast event to Redis Pub/Sub for cross-node gateway fanout.
     *
     * @param event The event to ingest
     * @return The sequenced event
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

        // 1. Strict atomic sequence allocation per topic
        Long seq = redisTemplate.opsForValue().increment(getSequenceKey(topic));
        long sequenceNumber = (seq != null) ? seq : 1L;
        event.setSequenceNumber(sequenceNumber);

        try {
            String eventJson = objectMapper.writeValueAsString(event);

            // 2. Add to bounded replay buffer (ZSET: score = sequenceNumber)
            String bufferKey = getBufferKey(topic);
            redisTemplate.opsForZSet().add(bufferKey, eventJson, (double) sequenceNumber);

            // Trim buffer: keep at most maxBufferSize elements (evict oldest from rank 0)
            redisTemplate.opsForZSet().removeRange(bufferKey, 0, -(maxBufferSize + 1));

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
     * Update the latest aggregated state snapshot in Redis for this topic.
     */
    private synchronized void updateSnapshot(String topic, Event event) {
        String snapshotKey = getSnapshotKey(topic);
        try {
            TopicSnapshot snapshot = null;
            String existingJson = redisTemplate.opsForValue().get(snapshotKey);
            if (existingJson != null) {
                snapshot = objectMapper.readValue(existingJson, TopicSnapshot.class);
            }
            if (snapshot == null) {
                snapshot = TopicSnapshot.builder().topic(topic).build();
            }

            // Only update if this event is current or newer
            if (event.getSequenceNumber() >= snapshot.getLastSequenceNumber()) {
                snapshot.applyEvent(event);
                String updatedJson = objectMapper.writeValueAsString(snapshot);
                redisTemplate.opsForValue().set(snapshotKey, updatedJson);
            }
        } catch (Exception e) {
            log.error("Failed to update snapshot for topic {}", topic, e);
        }
    }

    /**
     * Returns the current latest sequence number for a topic (0 if none).
     */
    public long getCurrentSequenceNumber(String topic) {
        String seqStr = redisTemplate.opsForValue().get(getSequenceKey(topic));
        return (seqStr != null) ? Long.parseLong(seqStr) : 0L;
    }

    /**
     * Returns the current number of events stored in the replay buffer for a topic.
     */
    public long getBufferSize(String topic) {
        Long size = redisTemplate.opsForZSet().size(getBufferKey(topic));
        return (size != null) ? size : 0L;
    }
}

