package com.moveinsync.tripfanout.security;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock authentication service to resolve user roles, IDs, and tenant assignments
 * from tokens or handshake/STOMP headers.
 */
@Slf4j
@Service
public class MockAuthenticationService {

    private final Map<String, UserPrincipal> tokenUserMap = new ConcurrentHashMap<>();
    private final Map<String, UserPrincipal> userIdMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void initPreconfiguredUsers() {
        registerUser("token-emp-1", UserPrincipal.builder()
                .userId("emp-1")
                .tenantId("TENANT-A")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-101")
                .assignedOfficeId("OFFICE-BLR")
                .build());

        registerUser("token-emp-2", UserPrincipal.builder()
                .userId("emp-2")
                .tenantId("TENANT-A")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-102")
                .assignedOfficeId("OFFICE-BLR")
                .build());

        registerUser("token-emp-other-tenant", UserPrincipal.builder()
                .userId("emp-other-tenant")
                .tenantId("TENANT-B")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-201")
                .assignedOfficeId("OFFICE-NYC")
                .build());

        registerUser("token-admin-blr", UserPrincipal.builder()
                .userId("admin-blr")
                .tenantId("TENANT-A")
                .role(UserRole.OFFICE_ADMIN)
                .assignedOfficeId("OFFICE-BLR")
                .build());

        registerUser("token-admin-hyd", UserPrincipal.builder()
                .userId("admin-hyd")
                .tenantId("TENANT-A")
                .role(UserRole.OFFICE_ADMIN)
                .assignedOfficeId("OFFICE-HYD")
                .build());

        registerUser("token-admin-nyc", UserPrincipal.builder()
                .userId("admin-other-tenant")
                .tenantId("TENANT-B")
                .role(UserRole.OFFICE_ADMIN)
                .assignedOfficeId("OFFICE-NYC")
                .build());

        registerUser("token-security-tenant-a", UserPrincipal.builder()
                .userId("security-tenant-a")
                .tenantId("TENANT-A")
                .role(UserRole.SECURITY_DESK)
                .build());

        registerUser("token-security-tenant-b", UserPrincipal.builder()
                .userId("security-tenant-b")
                .tenantId("TENANT-B")
                .role(UserRole.SECURITY_DESK)
                .build());

        registerUser("token-sys-admin", UserPrincipal.builder()
                .userId("sys-admin")
                .tenantId("MOVEINSYNC")
                .role(UserRole.SYSTEM_ADMIN)
                .build());
    }

    public void registerUser(String token, UserPrincipal principal) {
        tokenUserMap.put(token, principal);
        userIdMap.put(principal.getUserId(), principal);
    }

    /**
     * Resolves authentication from token or explicit header map.
     */
    public Optional<UserPrincipal> authenticate(String tokenOrAuthHeader, Map<String, String> headers) {
        // 1. Try Token
        if (tokenOrAuthHeader != null && !tokenOrAuthHeader.isBlank()) {
            String token = tokenOrAuthHeader.trim();
            if (token.startsWith("Bearer ")) {
                token = token.substring(7).trim();
            }
            UserPrincipal principal = tokenUserMap.get(token);
            if (principal != null) {
                return Optional.of(principal);
            }
            // Fallback: check if the token is directly a known userId
            UserPrincipal userById = userIdMap.get(token);
            if (userById != null) {
                return Optional.of(userById);
            }
        }

        // 2. Try explicit headers (X-User-Id, X-Tenant-Id, X-User-Role, etc.)
        if (headers != null && headers.containsKey("X-User-Id")) {
            String userId = headers.get("X-User-Id");
            UserPrincipal existing = userIdMap.get(userId);
            if (existing != null) {
                return Optional.of(existing);
            }

            // Construct on the fly if full details provided in headers
            String tenantId = headers.getOrDefault("X-Tenant-Id", "DEFAULT-TENANT");
            String roleStr = headers.getOrDefault("X-User-Role", "EMPLOYEE");
            UserRole role;
            try {
                role = UserRole.valueOf(roleStr.toUpperCase());
            } catch (Exception e) {
                role = UserRole.EMPLOYEE;
            }

            UserPrincipal constructed = UserPrincipal.builder()
                    .userId(userId)
                    .tenantId(tenantId)
                    .role(role)
                    .assignedTripId(headers.get("X-Assigned-Trip"))
                    .assignedOfficeId(headers.get("X-Assigned-Office"))
                    .build();
            return Optional.of(constructed);
        }

        return Optional.empty();
    }
}

