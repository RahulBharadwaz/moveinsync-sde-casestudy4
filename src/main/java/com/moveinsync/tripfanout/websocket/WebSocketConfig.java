package com.moveinsync.tripfanout.websocket;

import com.moveinsync.tripfanout.security.WebSocketSecurityInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket Gateway configuration for serving live updates to connected screens
 * (security desks, admins, employees) with STOMP message brokering.
 */
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final WebSocketSecurityInterceptor securityInterceptor;
    private final CustomStompErrorHandler errorHandler;

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // Simple in-memory broker handling subscriptions and broadcasts
        registry.enableSimpleBroker("/topic", "/queue");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // SockJS fallback for browsers
        registry.addEndpoint("/ws-trip-fanout")
                .setAllowedOriginPatterns("*")
                .withSockJS();

        // Native WebSocket endpoint
        registry.addEndpoint("/ws-trip-fanout-native")
                .setAllowedOriginPatterns("*");

        registry.setErrorHandler(errorHandler);
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(securityInterceptor);
    }
}

