package com.moveinsync.tripfanout.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripfanout.model.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes sequenced events to Redis Pub/Sub channels for cross-node fanout.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisEventPublisher implements EventPublisher {

    public static final String CHANNEL_PREFIX = "events:topic:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public String getChannelForTopic(String topic) {
        return CHANNEL_PREFIX + topic;
    }

    @Override
    public void publish(Event event) {
        if (event == null || event.getTopic() == null) {
            throw new IllegalArgumentException("Event and event topic cannot be null");
        }
        try {
            String channel = getChannelForTopic(event.getTopic());
            String payload = objectMapper.writeValueAsString(event);
            redisTemplate.convertAndSend(channel, payload);
            log.debug("Broadcasted event [{}] on topic [{}] (seq: {}) to channel [{}]",
                    event.getEventId(), event.getTopic(), event.getSequenceNumber(), channel);
        } catch (Exception e) {
            log.error("Failed to publish event to Redis channel: {}", event, e);
            throw new RuntimeException("Failed to broadcast event across cluster", e);
        }
    }
}

