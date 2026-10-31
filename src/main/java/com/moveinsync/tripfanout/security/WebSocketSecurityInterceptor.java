package com.moveinsync.tripfanout.security;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.HydrationResult;
import com.moveinsync.tripfanout.service.StateHydrationService;
import com.moveinsync.tripfanout.websocket.TopicUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Channel interceptor for WebSocket STOMP inbound frames.
 * - Authenticates clients during CONNECT.
 * - Enforces dynamic tenant isolation and role authorization during SUBSCRIBE.
 * - Registers subscriptions with the connection queue manager.
 * - Triggers state hydration if resuming with a sequence number.
 */
@Slf4j
@Component
public class WebSocketSecurityInterceptor implements ChannelInterceptor {

    private final MockAuthenticationService authenticationService;
    private final SubscriptionAuthorizationService authorizationService;
    private final StateHydrationService stateHydrationService;
    private final GatewayQueueManager gatewayQueueManager;
    private final SimpMessagingTemplate messagingTemplate;

    public WebSocketSecurityInterceptor(
            MockAuthenticationService authenticationService,
            SubscriptionAuthorizationService authorizationService,
            StateHydrationService stateHydrationService,
            GatewayQueueManager gatewayQueueManager,
            @Lazy SimpMessagingTemplate messagingTemplate) {
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.stateHydrationService = stateHydrationService;
        this.gatewayQueueManager = gatewayQueueManager;
        this.messagingTemplate = messagingTemplate;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || accessor.getCommand() == null) {
            return message;
        }

        if (StompCommand.CONNECT.equals(accessor.getCommand())) {
            return handleConnect(message, accessor);
        } else if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            handleSubscribe(accessor);
        } else if (StompCommand.UNSUBSCRIBE.equals(accessor.getCommand())) {
            handleUnsubscribe(accessor);
        } else if (StompCommand.DISCONNECT.equals(accessor.getCommand())) {
            handleDisconnect(accessor);
        }

        return message;
    }

    private Message<?> handleConnect(Message<?> message, StompHeaderAccessor accessor) {
        String authHeader = accessor.getFirstNativeHeader("Authorization");
        if (authHeader == null) {
            authHeader = accessor.getFirstNativeHeader("token");
        }

        Map<String, String> customHeaders = new HashMap<>();
        putIfPresent(accessor, "X-User-Id", customHeaders);
        putIfPresent(accessor, "X-Tenant-Id", customHeaders);
        putIfPresent(accessor, "X-User-Role", customHeaders);
        putIfPresent(accessor, "X-Assigned-Trip", customHeaders);
        putIfPresent(accessor, "X-Assigned-Office", customHeaders);

        Optional<UserPrincipal> principalOpt = authenticationService.authenticate(authHeader, customHeaders);
        if (principalOpt.isPresent()) {
            UserPrincipal principal = principalOpt.get();
            log.info("WebSocket connected user: [{}] (role: {}, tenant: {})",
                    principal.getUserId(), principal.getRole(), principal.getTenantId());

            if (accessor.isMutable()) {
                accessor.setUser(principal);
                return message;
            } else {
                StompHeaderAccessor mutableAccessor = StompHeaderAccessor.wrap(message);
                mutableAccessor.setUser(principal);
                return MessageBuilder.createMessage(message.getPayload(), mutableAccessor.getMessageHeaders());
            }
        } else {
            log.warn("Unauthenticated WebSocket connection attempt (no matching credentials)");
            return message;
        }
    }

    private void handleSubscribe(StompHeaderAccessor accessor) {
        Principal principal = accessor.getUser();
        String destination = accessor.getDestination();
        String sessionId = accessor.getSessionId();

        // 1. Dynamic Authorization & Tenant Isolation Check
        authorizationService.authorizeSubscription(principal, destination);
        log.info("Subscription approved: user=[{}] -> destination=[{}]",
                (principal != null ? principal.getName() : "anonymous"), destination);

        // 2. Register with Gateway Connection Queue Manager
        if (sessionId != null && destination != null) {
            gatewayQueueManager.registerSubscription(sessionId, destination);
        }

        // 3. Resumable State Hydration (if client passes last-sequence-number header)
        String lastSeqHeader = accessor.getFirstNativeHeader("last-sequence-number");
        if (lastSeqHeader == null) {
            lastSeqHeader = accessor.getFirstNativeHeader("lastSequenceNumber");
        }

        if (lastSeqHeader != null && stateHydrationService != null && messagingTemplate != null) {
            try {
                long lastSeq = Long.parseLong(lastSeqHeader.trim());
                String canonicalTopic = TopicUtils.toCanonicalTopic(destination);
                HydrationResult hydration = stateHydrationService.hydrate(canonicalTopic, lastSeq, true);

                log.info("Hydrating resuming subscriber [{}]: topic=[{}], requestedSeq=[{}], type=[{}]",
                        (principal != null ? principal.getName() : "anonymous"), canonicalTopic, lastSeq, hydration.getType());

                ClientSessionQueue clientQueue = (sessionId != null) ? gatewayQueueManager.getOrCreateQueue(sessionId) : null;

                if (hydration.getType() == HydrationResult.HydrationType.REPLAY) {
                    for (Event missed : hydration.getMissedEvents()) {
                        if (clientQueue != null) {
                            clientQueue.enqueue(missed);
                        }
                        deliverToSession(sessionId, destination, accessor.getSubscriptionId(), missed);
                    }
                } else if (hydration.getType() == HydrationResult.HydrationType.SNAPSHOT && hydration.getSnapshot() != null) {
                    deliverToSession(sessionId, destination, accessor.getSubscriptionId(), hydration.getSnapshot());
                }
            } catch (Exception e) {
                log.warn("Failed to process resumable hydration on subscribe for destination {}", destination, e);
            }
        }
    }

    private void deliverToSession(String sessionId, String destination, String subscriptionId, Object payload) {
        if (messagingTemplate == null || destination == null) {
            return;
        }
        StompHeaderAccessor replyHeaders = StompHeaderAccessor.create(StompCommand.MESSAGE);
        replyHeaders.setDestination(destination);
        if (sessionId != null) {
            replyHeaders.setSessionId(sessionId);
        }
        if (subscriptionId != null) {
            replyHeaders.setSubscriptionId(subscriptionId);
        }
        replyHeaders.setNativeHeader("X-Hydrated", "true");
        replyHeaders.setLeaveMutable(true);
        messagingTemplate.convertAndSend(destination, payload, replyHeaders.getMessageHeaders());
    }

    private void handleUnsubscribe(StompHeaderAccessor accessor) {
        String sessionId = accessor.getSessionId();
        String destination = accessor.getDestination();
        if (sessionId != null && destination != null) {
            gatewayQueueManager.unregisterSubscription(sessionId, destination);
        }
    }

    private void handleDisconnect(StompHeaderAccessor accessor) {
        String sessionId = accessor.getSessionId();
        if (sessionId != null) {
            gatewayQueueManager.removeSession(sessionId);
        }
    }

    private void putIfPresent(StompHeaderAccessor accessor, String headerName, Map<String, String> map) {
        String val = accessor.getFirstNativeHeader(headerName);
        if (val != null) {
            map.put(headerName, val);
        }
    }
}
