package com.moveinsync.tripfanout.security;

/**
 * Thrown when a subscriber attempts to subscribe to an unauthorized destination,
 * violating role permissions or tenant isolation.
 */
public class SubscriptionSecurityException extends RuntimeException {

    public SubscriptionSecurityException(String message) {
        super(message);
    }
}

