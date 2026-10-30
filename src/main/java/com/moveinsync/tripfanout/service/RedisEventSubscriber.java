package com.moveinsync.tripfanout.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.tripfanout.model.Event;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Listens to Redis Pub/Sub channels for cross-node event distribution.
 * Forwards received events to registered in-process handlers (e.g. WebSocket connection dispatchers).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisEventSubscriber implements MessageListener {

    public static final String CHANNEL_PATTERN = "events:topic:*";

    private final RedisMessageListenerContainer listenerContainer;
    private final ObjectMapper objectMapper;
    private final List<Consumer<Event>> eventListeners = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        listenerContainer.addMessageListener(this, new PatternTopic(CHANNEL_PATTERN));
        log.info("Subscribed to cross-node Redis channel pattern: {}", CHANNEL_PATTERN);
    }

    public void addListener(Consumer<Event> listener) {
        eventListeners.add(listener);
    }

    public void removeListener(Consumer<Event> listener) {
        eventListeners.remove(listener);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            Event event = objectMapper.readValue(body, Event.class);
            log.debug("Received event via cross-node Pub/Sub on channel {}: seq={}", channel, event.getSequenceNumber());

            for (Consumer<Event> listener : eventListeners) {
                try {
                    listener.accept(event);
                } catch (Exception ex) {
                    log.error("Error executing listener callback for event {}", event.getEventId(), ex);
                }
            }
        } catch (Exception e) {
            log.error("Failed to deserialize or dispatch event from Redis message", e);
        }
    }
}

