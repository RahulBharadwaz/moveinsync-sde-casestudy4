package com.moveinsync.tripfanout.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.StompSubProtocolErrorHandler;

import java.nio.charset.StandardCharsets;

/**
 * Handles errors occurring during STOMP frame processing (including subscription authorization failures)
 * by returning structured, non-leaking STOMP ERROR frames to the client.
 */
@Slf4j
@Component
public class CustomStompErrorHandler extends StompSubProtocolErrorHandler {

    @Override
    public Message<byte[]> handleClientMessageProcessingError(Message<byte[]> clientMessage, Throwable ex) {
        Throwable cause = ex;
        while (cause != null && cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }

        String rawMessage = (cause != null && cause.getMessage() != null) ? cause.getMessage() : "Internal WebSocket processing error";
        String sanitizedMessage = sanitize(rawMessage);

        log.warn("STOMP processing error: {}", sanitizedMessage);

        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.ERROR);
        accessor.setMessage(sanitizedMessage);
        accessor.setLeaveMutable(true);

        return MessageBuilder.createMessage(
                sanitizedMessage.getBytes(StandardCharsets.UTF_8),
                accessor.getMessageHeaders()
        );
    }

    private String sanitize(String message) {
        if (message == null || message.isBlank()) {
            return "Request could not be processed";
        }
        String sanitized = message.replaceAll("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+", "[REDACTED_EMAIL]");
        sanitized = sanitized.replaceAll("(?<!\\d)\\d{10}(?!\\d)", "[REDACTED_PHONE]");
        return sanitized;
    }
}
