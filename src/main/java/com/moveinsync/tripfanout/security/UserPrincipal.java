package com.moveinsync.tripfanout.security;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.security.Principal;

/**
 * Authenticated user principal for WebSocket connections.
 * Enforces role-based access control and tenant isolation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserPrincipal implements Principal {

    private String userId;
    private String tenantId;
    private UserRole role;
    private String assignedTripId;
    private String assignedOfficeId;

    @Override
    public String getName() {
        return userId;
    }
}

