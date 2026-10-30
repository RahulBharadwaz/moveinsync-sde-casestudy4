package com.moveinsync.tripfanout.engine;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.websocket.TopicUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

/**
 * Manages per-connection queues and topic subscriptions for all connected screens on this gateway node.
 * Coordinates non-blocking fan-out with back-pressure isolation and conflation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GatewayQueueManager {

    private final SlowConsumerPolicy policy;

    // sessionId -> ClientSessionQueue
    private final Map<String, ClientSessionQueue> sessionQueues = new ConcurrentHashMap<>();

    // canonicalTopic -> Set of sessionIds
    private final Map<String, Set<String>> topicSubscriptions = new ConcurrentHashMap<>();

    // sessionId -> Set of subscribed canonicalTopics
    private final Map<String, Set<String>> sessionTopics = new ConcurrentHashMap<>();

    // Optional listener invoked when an event is queued for a session
    private BiConsumer<String, Event> onEventQueuedListener;

    public void setOnEventQueuedListener(BiConsumer<String, Event> listener) {
        this.onEventQueuedListener = listener;
    }

    /**
     * Get or create an isolated queue for a client session.
     */
    public ClientSessionQueue getOrCreateQueue(String sessionId) {
        return sessionQueues.computeIfAbsent(sessionId, id ->
                new ClientSessionQueue(id, policy.getMaxCapacity(), policy.isConflationEnabled()));
    }

    public Optional<ClientSessionQueue> getQueue(String sessionId) {
        return Optional.ofNullable(sessionQueues.get(sessionId));
    }

    /**
     * Register a client session subscription to a topic destination.
     */
    public void registerSubscription(String sessionId, String destination) {
        String canonicalTopic = TopicUtils.toCanonicalTopic(destination);
        getOrCreateQueue(sessionId);

        topicSubscriptions.computeIfAbsent(canonicalTopic, k -> new CopyOnWriteArraySet<>()).add(sessionId);
        sessionTopics.computeIfAbsent(sessionId, k -> new CopyOnWriteArraySet<>()).add(canonicalTopic);

        log.debug("Session [{}] registered subscription to [{}] (canonical: {})",
                sessionId, destination, canonicalTopic);
    }

    /**
     * Unsubscribe a session from a topic.
     */
    public void unregisterSubscription(String sessionId, String destination) {
        String canonicalTopic = TopicUtils.toCanonicalTopic(destination);
        Set<String> sessions = topicSubscriptions.get(canonicalTopic);
        if (sessions != null) {
            sessions.remove(sessionId);
            if (sessions.isEmpty()) {
                topicSubscriptions.remove(canonicalTopic);
            }
        }
        Set<String> topics = sessionTopics.get(sessionId);
        if (topics != null) {
            topics.remove(canonicalTopic);
        }
    }

    /**
     * Cleanup session on disconnect.
     */
    public void removeSession(String sessionId) {
        Set<String> topics = sessionTopics.remove(sessionId);
        if (topics != null) {
            for (String topic : topics) {
                Set<String> sessions = topicSubscriptions.get(topic);
                if (sessions != null) {
                    sessions.remove(sessionId);
                    if (sessions.isEmpty()) {
                        topicSubscriptions.remove(topic);
                    }
                }
            }
        }
        ClientSessionQueue removed = sessionQueues.remove(sessionId);
        if (removed != null) {
            removed.clear();
            log.debug("Cleaned up queue for session [{}]", sessionId);
        }
    }

    /**
     * Fan-out an event non-blockingly to all subscribed connection queues.
     * Enqueuing is strictly isolated so a slow connection never slows down other connections.
     *
     * @param event The event to fan out
     * @return Number of sessions event was enqueued to
     */
    public int fanout(Event event) {
        if (event == null || event.getTopic() == null) {
            return 0;
        }

        String canonicalTopic = TopicUtils.toCanonicalTopic(event.getTopic());
        Set<String> subscribers = topicSubscriptions.get(canonicalTopic);

        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }

        int deliveredCount = 0;
        for (String sessionId : subscribers) {
            ClientSessionQueue queue = sessionQueues.get(sessionId);
            if (queue != null) {
                queue.enqueue(event);
                deliveredCount++;
                if (onEventQueuedListener != null) {
                    try {
                        onEventQueuedListener.accept(sessionId, event);
                    } catch (Exception e) {
                        log.error("Error in onEventQueuedListener for session {}", sessionId, e);
                    }
                }
            }
        }

        log.trace("Fanned out event [{}] on topic [{}] to [{}] connection queues",
                event.getEventId(), canonicalTopic, deliveredCount);
        return deliveredCount;
    }

    public int getActiveSessionCount() {
        return sessionQueues.size();
    }

    public long getTotalConflatedCount() {
        return sessionQueues.values().stream()
                .mapToLong(q -> q.getConflatedCount().get())
                .sum();
    }

    public long getTotalDroppedCount() {
        return sessionQueues.values().stream()
                .mapToLong(q -> q.getDroppedLocationCount().get())
                .sum();
    }

    public long getBackPressuredSessionCount() {
        return sessionQueues.values().stream()
                .filter(ClientSessionQueue::isBackPressured)
                .count();
    }
}

