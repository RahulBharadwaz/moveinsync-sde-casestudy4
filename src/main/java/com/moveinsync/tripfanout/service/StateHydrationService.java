package com.moveinsync.tripfanout.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.HydrationResult;
import com.moveinsync.tripfanout.model.TopicSnapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Service for hydrating reconnecting subscribers.
 * Replays missed events in order if within the bounded buffer; otherwise hydrates with full current state snapshot.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StateHydrationService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Hydrate a client resuming subscription on a topic.
     *
     * @param topic Topic identifier
     * @param lastSequenceNumber Last sequence number successfully processed by the client
     * @return HydrationResult containing either REPLAY list or full SNAPSHOT
     */
    public HydrationResult hydrate(String topic, long lastSequenceNumber) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }

        String seqKey = EventSequencingService.getSequenceKey(topic);
        String seqStr = redisTemplate.opsForValue().get(seqKey);
        long currentSeq = (seqStr != null) ? Long.parseLong(seqStr) : 0L;

        // 1. Topic has no events yet
        if (currentSeq == 0) {
            return HydrationResult.replay(topic, lastSequenceNumber, 0, Collections.emptyList());
        }

        // 2. Client is already fully caught up
        if (lastSequenceNumber >= currentSeq) {
            return HydrationResult.replay(topic, lastSequenceNumber, currentSeq, Collections.emptyList());
        }

        // 3. Inspect bounded buffer
        String bufferKey = EventSequencingService.getBufferKey(topic);
        Set<ZSetOperations.TypedTuple<String>> minTuples = redisTemplate.opsForZSet().rangeWithScores(bufferKey, 0, 0);

        if (minTuples == null || minTuples.isEmpty()) {
            // Buffer is empty even though currentSeq > 0 (e.g. purged), client must get snapshot
            log.info("Buffer empty for topic {}, falling back to full snapshot hydration", topic);
            return HydrationResult.snapshot(topic, lastSequenceNumber, currentSeq, getSnapshot(topic));
        }

        Double minScore = minTuples.iterator().next().getScore();
        long minSeq = (minScore != null) ? minScore.longValue() : 0L;

        // 4. Check if the next event the client needs (lastSequenceNumber + 1) has been evicted
        // i.e. lastSequenceNumber < minSeq - 1
        if (lastSequenceNumber < minSeq - 1) {
            log.info("Client requested seq {} on topic {}, but buffer min seq is {}. Evicted! Hydrating snapshot.",
                    lastSequenceNumber, topic, minSeq);
            TopicSnapshot snapshot = getSnapshot(topic);
            return HydrationResult.snapshot(topic, lastSequenceNumber, currentSeq, snapshot);
        }

        // 5. In-buffer replay: Fetch all missed events in score (sequence) order
        Set<String> eventJsons = redisTemplate.opsForZSet().rangeByScore(
                bufferKey,
                (double) (lastSequenceNumber + 1),
                Double.POSITIVE_INFINITY
        );

        List<Event> missedEvents = new ArrayList<>();
        if (eventJsons != null) {
            for (String json : eventJsons) {
                try {
                    missedEvents.add(objectMapper.readValue(json, Event.class));
                } catch (Exception e) {
                    log.error("Failed to deserialize buffered event JSON for topic {}", topic, e);
                }
            }
        }

        log.debug("Replaying {} missed events for topic {} from seq {} to current {}",
                missedEvents.size(), topic, lastSequenceNumber, currentSeq);

        return HydrationResult.replay(topic, lastSequenceNumber, currentSeq, missedEvents);
    }

    /**
     * Retrieve the current aggregated state snapshot for a topic.
     */
    public TopicSnapshot getSnapshot(String topic) {
        String snapshotKey = EventSequencingService.getSnapshotKey(topic);
        String json = redisTemplate.opsForValue().get(snapshotKey);
        if (json != null) {
            try {
                return objectMapper.readValue(json, TopicSnapshot.class);
            } catch (Exception e) {
                log.error("Failed to parse snapshot for topic {}", topic, e);
            }
        }
        String seqStr = redisTemplate.opsForValue().get(EventSequencingService.getSequenceKey(topic));
        long currentSeq = (seqStr != null) ? Long.parseLong(seqStr) : 0L;
        return TopicSnapshot.builder()
                .topic(topic)
                .lastSequenceNumber(currentSeq)
                .build();
    }
}

