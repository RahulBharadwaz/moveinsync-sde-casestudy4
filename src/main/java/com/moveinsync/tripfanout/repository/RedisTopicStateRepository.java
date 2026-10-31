package com.moveinsync.tripfanout.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.TopicSnapshot;
import com.moveinsync.tripfanout.repository.cache.BoundedLruCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Production Redis implementation of TopicStateRepository.
 * Manages atomic sequence generation, bounded ZSet sliding buffer,
 * and materialized topic snapshot storage with L1 hot-data caching.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisTopicStateRepository implements TopicStateRepository {

    public static final String SEQ_KEY_PREFIX = "topic:%s:seq";
    public static final String BUFFER_KEY_PREFIX = "topic:%s:buffer";
    public static final String SNAPSHOT_KEY_PREFIX = "topic:%s:snapshot";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    // L1 Hot Snapshot Cache: 5,000 entries max, 2,000ms TTL (guarantees max 2s bounded staleness)
    private final BoundedLruCache<String, TopicSnapshot> l1SnapshotCache =
            new BoundedLruCache<>(5000, 2000L);

    public static String getSequenceKey(String topic) {
        return String.format(SEQ_KEY_PREFIX, topic);
    }

    public static String getBufferKey(String topic) {
        return String.format(BUFFER_KEY_PREFIX, topic);
    }

    public static String getSnapshotKey(String topic) {
        return String.format(SNAPSHOT_KEY_PREFIX, topic);
    }

    @Override
    public long incrementSequence(String topic) {
        Long seq = redisTemplate.opsForValue().increment(getSequenceKey(topic));
        return (seq != null) ? seq : 1L;
    }

    @Override
    public long getCurrentSequence(String topic) {
        String seqStr = redisTemplate.opsForValue().get(getSequenceKey(topic));
        return (seqStr != null) ? Long.parseLong(seqStr) : 0L;
    }

    @Override
    public void appendToBuffer(String topic, Event event, long sequenceNumber, int maxBufferSize) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            String bufferKey = getBufferKey(topic);
            redisTemplate.opsForZSet().add(bufferKey, eventJson, (double) sequenceNumber);
            // Trim buffer: keep at most maxBufferSize elements (evict oldest from rank 0)
            redisTemplate.opsForZSet().removeRange(bufferKey, 0, -(maxBufferSize + 1));
        } catch (Exception e) {
            log.error("Failed to append event [{}] to buffer for topic [{}]", event.getEventId(), topic, e);
            throw new RuntimeException("Replay buffer write failed", e);
        }
    }

    @Override
    public List<Event> getBufferRange(String topic, long minSeq, long maxSeq) {
        String bufferKey = getBufferKey(topic);
        double maxScore = (maxSeq == Long.MAX_VALUE) ? Double.POSITIVE_INFINITY : (double) maxSeq;
        Set<String> eventJsons = redisTemplate.opsForZSet().rangeByScore(bufferKey, (double) minSeq, maxScore);

        List<Event> events = new ArrayList<>();
        if (eventJsons != null) {
            for (String json : eventJsons) {
                try {
                    events.add(objectMapper.readValue(json, Event.class));
                } catch (Exception e) {
                    log.error("Failed to deserialize buffered event JSON for topic [{}]", topic, e);
                }
            }
        }
        return events;
    }

    @Override
    public Long getBufferMinSequence(String topic) {
        String bufferKey = getBufferKey(topic);
        Set<ZSetOperations.TypedTuple<String>> minTuples = redisTemplate.opsForZSet().rangeWithScores(bufferKey, 0, 0);
        if (minTuples == null || minTuples.isEmpty()) {
            return null;
        }
        Double minScore = minTuples.iterator().next().getScore();
        return (minScore != null) ? minScore.longValue() : null;
    }

    @Override
    public long getBufferSize(String topic) {
        Long size = redisTemplate.opsForZSet().size(getBufferKey(topic));
        return (size != null) ? size : 0L;
    }

    @Override
    public void saveSnapshot(String topic, TopicSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        try {
            String snapshotJson = objectMapper.writeValueAsString(snapshot);
            redisTemplate.opsForValue().set(getSnapshotKey(topic), snapshotJson);
            // Update L1 Cache
            l1SnapshotCache.put(topic, snapshot);
        } catch (Exception e) {
            log.error("Failed to serialize snapshot for topic [{}]", topic, e);
        }
    }

    @Override
    public Optional<TopicSnapshot> getSnapshot(String topic) {
        // Check L1 Hot Cache first
        Optional<TopicSnapshot> cached = l1SnapshotCache.get(topic);
        if (cached.isPresent()) {
            return cached;
        }

        String json = redisTemplate.opsForValue().get(getSnapshotKey(topic));
        if (json != null) {
            try {
                TopicSnapshot snapshot = objectMapper.readValue(json, TopicSnapshot.class);
                l1SnapshotCache.put(topic, snapshot);
                return Optional.of(snapshot);
            } catch (Exception e) {
                log.error("Failed to parse snapshot JSON for topic [{}]", topic, e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void clearTopic(String topic) {
        redisTemplate.delete(getSequenceKey(topic));
        redisTemplate.delete(getBufferKey(topic));
        redisTemplate.delete(getSnapshotKey(topic));
        l1SnapshotCache.invalidate(topic);
    }
}

