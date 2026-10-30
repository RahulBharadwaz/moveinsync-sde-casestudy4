package com.moveinsync.tripfanout.websocket;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.service.RedisEventSubscriber;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * The Bridge: Subscribes to cluster-wide events from Redis Pub/Sub (Phase 2)
 * and fans them out to connected WebSocket subscribers on the local gateway node.
 * Integrates directly with GatewayQueueManager for bounded back-pressure and conflation.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisToWebSocketBridge {

    private final RedisEventSubscriber redisEventSubscriber;
    private final SimpMessagingTemplate messagingTemplate;
    private final GatewayQueueManager gatewayQueueManager;

    @PostConstruct
    public void init() {
        redisEventSubscriber.addListener(this::routeToWebSocket);
        log.info("Redis to WebSocket Bridge initialized and listening for cluster events");
    }

    /**
     * Routes an event received from Redis Pub/Sub:
     * 1. Non-blockingly enqueues into all subscribed connection queues (conflating and prioritizing).
     * 2. Broadcasts to STOMP topic destinations.
     *
     * @param event The event received from Redis
     */
    public void routeToWebSocket(Event event) {
        if (event == null || event.getTopic() == null || event.getTopic().isBlank()) {
            log.warn("Received null or untopiced event from Redis, dropping");
            return;
        }

        // 1. Fan-out to per-connection bounded queues with conflation & priority lanes
        gatewayQueueManager.fanout(event);

        // 2. Route to standard STOMP destinations
        String primaryDestination = TopicUtils.toDestination(event.getTopic());
        messagingTemplate.convertAndSend(primaryDestination, event);

        // Also route to colon format if distinct (e.g. /topic/trip:TRIP-101)
        String colonDestination = "/topic/" + event.getTopic();
        if (!colonDestination.equals(primaryDestination)) {
            messagingTemplate.convertAndSend(colonDestination, event);
        }

        log.debug("Bridged event [{}] (seq: {}, type: {}) to WebSocket destination [{}]",
                event.getEventId(), event.getSequenceNumber(), event.getEventType(), primaryDestination);
    }
}
