package com.moveinsync.tripfanout.exception;

import com.moveinsync.tripfanout.security.SubscriptionSecurityException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * Global REST exception handler that intercepts exceptions across the gateway
 * and converts them into non-leaking, sanitized error responses.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SubscriptionSecurityException.class)
    public ResponseEntity<ErrorResponse> handleSecurityException(SubscriptionSecurityException ex, HttpServletRequest request) {
        log.warn("Security rejection on path [{}]: {}", request.getRequestURI(), ex.getMessage());
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.FORBIDDEN.value())
                .error("FORBIDDEN")
                .message(sanitizeMessage(ex.getMessage()))
                .path(request.getRequestURI())
                .build();
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        log.warn("Bad request on path [{}]: {}", request.getRequestURI(), ex.getMessage());
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("BAD_REQUEST")
                .message(sanitizeMessage(ex.getMessage()))
                .path(request.getRequestURI())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex, HttpServletRequest request) {
        log.error("Internal server error on path [{}]", request.getRequestURI(), ex);
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .error("INTERNAL_SERVER_ERROR")
                .message("An internal error occurred. Please contact system administrator.")
                .path(request.getRequestURI())
                .build();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    /**
     * Sanitizes error messages to strip any potential personal identifiers or raw stack traces.
     */
    private String sanitizeMessage(String message) {
        if (message == null || message.isBlank()) {
            return "Request could not be processed";
        }
        // Mask emails or phone numbers if ever inadvertently included
        String sanitized = message.replaceAll("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+", "[REDACTED_EMAIL]");
        sanitized = sanitized.replaceAll("(?<!\\d)\\d{10}(?!\\d)", "[REDACTED_PHONE]");
        return sanitized;
    }
}

