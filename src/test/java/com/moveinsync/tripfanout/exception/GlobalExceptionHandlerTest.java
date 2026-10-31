package com.moveinsync.tripfanout.exception;

import com.moveinsync.tripfanout.security.SubscriptionSecurityException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler exceptionHandler;
    private MockHttpServletRequest request;

    @BeforeEach
    void setUp() {
        exceptionHandler = new GlobalExceptionHandler();
        request = new MockHttpServletRequest();
        request.setRequestURI("/ws-trip-fanout/subscribe");
    }

    @Test
    @DisplayName("Security Exception: returns 403 Forbidden with sanitized, non-leaking message")
    void testHandleSecurityException() {
        SubscriptionSecurityException ex = new SubscriptionSecurityException("Tenant isolation violation: cannot access user john.doe@example.com with phone 9876543210");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleSecurityException(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody()).isNotNull();

        ErrorResponse body = response.getBody();
        assertThat(body.getStatus()).isEqualTo(403);
        assertThat(body.getError()).isEqualTo("FORBIDDEN");
        assertThat(body.getPath()).isEqualTo("/ws-trip-fanout/subscribe");

        // Verify PII is redacted
        assertThat(body.getMessage()).doesNotContain("john.doe@example.com");
        assertThat(body.getMessage()).doesNotContain("9876543210");
        assertThat(body.getMessage()).contains("[REDACTED_EMAIL]");
        assertThat(body.getMessage()).contains("[REDACTED_PHONE]");
    }

    @Test
    @DisplayName("Illegal Argument Exception: returns 400 Bad Request")
    void testHandleIllegalArgumentException() {
        IllegalArgumentException ex = new IllegalArgumentException("Invalid topic identifier format");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleIllegalArgument(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo(400);
        assertThat(response.getBody().getError()).isEqualTo("BAD_REQUEST");
        assertThat(response.getBody().getMessage()).isEqualTo("Invalid topic identifier format");
    }

    @Test
    @DisplayName("Generic Uncaught Exception: returns 500 without leaking stack traces or internal details")
    void testHandleGenericException() {
        NullPointerException ex = new NullPointerException("Internal null pointer in db connection string jdbc:postgresql://secret-host:5432");
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleGenericException(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isNotNull();

        ErrorResponse body = response.getBody();
        assertThat(body.getStatus()).isEqualTo(500);
        assertThat(body.getError()).isEqualTo("INTERNAL_SERVER_ERROR");
        // Internal details and stack trace must NOT be leaked
        assertThat(body.getMessage()).isEqualTo("An internal error occurred. Please contact system administrator.");
        assertThat(body.getMessage()).doesNotContain("secret-host");
    }
}

