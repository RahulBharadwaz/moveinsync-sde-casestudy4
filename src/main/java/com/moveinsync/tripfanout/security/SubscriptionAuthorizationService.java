package com.moveinsync.tripfanout.security;

import com.moveinsync.tripfanout.websocket.TopicUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.security.Principal;
import java.util.Optional;

/**
 * Service that enforces dynamic authorization and strict tenant isolation at the exact
 * moment of subscription.
 *
 * Rules:
 * 1. Employees can only subscribe to their own trip, and only within their tenant.
 * 2. Office Admins can only subscribe to their assigned office (or trips in their office/tenant).
 * 3. Security Desks can subscribe to trips/cabs/offices strictly within their tenant.
 * 4. Nobody can subscribe to another client's or tenant's data.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SubscriptionAuthorizationService {

    private final TripMetadataRegistry tripRegistry;

    /**
     * Authorize a subscription request dynamically.
     *
     * @param principal The authenticated user principal
     * @param destination The requested WebSocket topic destination
     * @throws SubscriptionSecurityException if unauthorized or tenant isolation is violated
     */
    public void authorizeSubscription(Principal principal, String destination) {
        if (principal == null) {
            log.warn("Unauthorized subscription attempt to destination [{}] by unauthenticated user", destination);
            throw new SubscriptionSecurityException("Unauthenticated: subscription requires valid credentials");
        }

        if (!(principal instanceof UserPrincipal user)) {
            log.warn("Unrecognized principal type: {}", principal.getClass().getName());
            throw new SubscriptionSecurityException("Invalid user principal");
        }

        if (destination == null || destination.isBlank()) {
            throw new SubscriptionSecurityException("Destination cannot be empty");
        }

        String canonicalTopic = TopicUtils.toCanonicalTopic(destination);
        String topicType = TopicUtils.extractType(canonicalTopic);
        String entityId = TopicUtils.extractId(canonicalTopic);

        log.debug("Authorizing subscription: user=[{}], role=[{}], tenant=[{}], topic=[{}]",
                user.getUserId(), user.getRole(), user.getTenantId(), canonicalTopic);

        // System Admin can observe everything
        if (user.getRole() == UserRole.SYSTEM_ADMIN) {
            return;
        }

        switch (user.getRole()) {
            case EMPLOYEE -> authorizeEmployee(user, topicType, entityId, destination);
            case OFFICE_ADMIN -> authorizeOfficeAdmin(user, topicType, entityId, destination);
            case SECURITY_DESK -> authorizeSecurityDesk(user, topicType, entityId, destination);
            default -> throw new SubscriptionSecurityException("Access denied: role [" + user.getRole() + "] has no subscription privileges");
        }
    }

    private void authorizeEmployee(UserPrincipal user, String topicType, String tripId, String destination) {
        if (!"trip".equalsIgnoreCase(topicType)) {
            log.warn("Employee [{}] blocked from subscribing to non-trip destination [{}]", user.getUserId(), destination);
            throw new SubscriptionSecurityException("Employees are only permitted to subscribe to trips");
        }

        Optional<TripMetadata> tripOpt = tripRegistry.findTrip(tripId);
        if (tripOpt.isEmpty()) {
            // For trips not yet in registry, check against user's assigned trip if present
            if (user.getAssignedTripId() != null && user.getAssignedTripId().equalsIgnoreCase(tripId)) {
                return;
            }
            log.warn("Employee [{}] attempted to subscribe to unknown trip [{}]", user.getUserId(), tripId);
            throw new SubscriptionSecurityException("Access denied: trip [" + tripId + "] not found or not assigned to employee");
        }

        TripMetadata trip = tripOpt.get();

        // 1. Strict Tenant Isolation
        if (!user.getTenantId().equalsIgnoreCase(trip.getTenantId())) {
            log.error("TENANT ISOLATION BREACH ATTEMPT: User [{}] (tenant: {}) attempted to subscribe to trip [{}] of tenant [{}]",
                    user.getUserId(), user.getTenantId(), tripId, trip.getTenantId());
            throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's data");
        }

        // 2. Own-Trip Restriction
        boolean isAssignedEmployee = user.getUserId().equalsIgnoreCase(trip.getEmployeeId()) ||
                (user.getAssignedTripId() != null && user.getAssignedTripId().equalsIgnoreCase(tripId));

        if (!isAssignedEmployee) {
            log.warn("Access denied: Employee [{}] attempted to subscribe to trip [{}] belonging to employee [{}]",
                    user.getUserId(), tripId, trip.getEmployeeId());
            throw new SubscriptionSecurityException("Employees can only subscribe to their own trip");
        }
    }

    private void authorizeOfficeAdmin(UserPrincipal user, String topicType, String entityId, String destination) {
        if ("office".equalsIgnoreCase(topicType)) {
            // Admin can only subscribe to their assigned office
            if (user.getAssignedOfficeId() == null || !user.getAssignedOfficeId().equalsIgnoreCase(entityId)) {
                log.warn("Office Admin [{}] denied subscription to unassigned office [{}]", user.getUserId(), entityId);
                throw new SubscriptionSecurityException("Office admins can only subscribe to their assigned office");
            }

            // Verify office belongs to admin's tenant
            Optional<String> officeTenant = tripRegistry.getTenantForOffice(entityId);
            if (officeTenant.isPresent() && !officeTenant.get().equalsIgnoreCase(user.getTenantId())) {
                log.error("TENANT ISOLATION BREACH: Admin [{}] attempted to access office [{}] of tenant [{}]",
                        user.getUserId(), entityId, officeTenant.get());
                throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's office");
            }
        } else if ("trip".equalsIgnoreCase(topicType)) {
            // Admin may monitor trips originating from/destined to their assigned office within their tenant
            Optional<TripMetadata> tripOpt = tripRegistry.findTrip(entityId);
            if (tripOpt.isEmpty()) {
                throw new SubscriptionSecurityException("Access denied: trip [" + entityId + "] not found");
            }
            TripMetadata trip = tripOpt.get();
            if (!user.getTenantId().equalsIgnoreCase(trip.getTenantId())) {
                throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's data");
            }
            if (user.getAssignedOfficeId() != null && !user.getAssignedOfficeId().equalsIgnoreCase(trip.getOfficeId())) {
                throw new SubscriptionSecurityException("Office admin can only subscribe to trips within office: " + user.getAssignedOfficeId());
            }
        } else {
            throw new SubscriptionSecurityException("Office admins cannot subscribe to [" + topicType + "] topics");
        }
    }

    private void authorizeSecurityDesk(UserPrincipal user, String topicType, String entityId, String destination) {
        // Security desks watch trips live: cab location, trip status changes, delays, and SOS alerts within their tenant
        switch (topicType.toLowerCase()) {
            case "trip" -> {
                Optional<TripMetadata> tripOpt = tripRegistry.findTrip(entityId);
                if (tripOpt.isPresent() && !tripOpt.get().getTenantId().equalsIgnoreCase(user.getTenantId())) {
                    throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's trip");
                }
            }
            case "office" -> {
                Optional<String> officeTenant = tripRegistry.getTenantForOffice(entityId);
                if (officeTenant.isPresent() && !officeTenant.get().equalsIgnoreCase(user.getTenantId())) {
                    throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's office");
                }
            }
            case "cab" -> {
                Optional<String> cabTenant = tripRegistry.getTenantForCab(entityId);
                if (cabTenant.isPresent() && !cabTenant.get().equalsIgnoreCase(user.getTenantId())) {
                    throw new SubscriptionSecurityException("Tenant isolation violation: cannot access another client's cab");
                }
            }
            default -> throw new SubscriptionSecurityException("Unsupported topic type [" + topicType + "]");
        }
    }
}

