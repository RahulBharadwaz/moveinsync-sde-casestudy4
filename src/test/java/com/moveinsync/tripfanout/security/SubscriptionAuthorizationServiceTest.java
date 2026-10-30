package com.moveinsync.tripfanout.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubscriptionAuthorizationServiceTest {

    private TripMetadataRegistry tripRegistry;
    private SubscriptionAuthorizationService authService;

    @BeforeEach
    void setUp() {
        tripRegistry = new TripMetadataRegistry();
        tripRegistry.initDefaults();
        authService = new SubscriptionAuthorizationService(tripRegistry);
    }

    @Nested
    @DisplayName("Employee Subscription Rules")
    class EmployeeRules {

        private final UserPrincipal emp1 = UserPrincipal.builder()
                .userId("emp-1")
                .tenantId("TENANT-A")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-101")
                .build();

        @Test
        @DisplayName("Employee can successfully subscribe to their own trip")
        void employeeCanSubscribeToOwnTrip() {
            assertThatCode(() -> authService.authorizeSubscription(emp1, "/topic/trip/TRIP-101"))
                    .doesNotThrowAnyException();

            // Also test canonical slash and colon formats
            assertThatCode(() -> authService.authorizeSubscription(emp1, "/topic/trip:TRIP-101"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Employee attempting to subscribe to another employee's trip in same tenant is blocked")
        void employeeCannotSubscribeToOtherEmployeeTrip() {
            assertThatThrownBy(() -> authService.authorizeSubscription(emp1, "/topic/trip/TRIP-102"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Employees can only subscribe to their own trip");
        }

        @Test
        @DisplayName("Employee attempting to subscribe to another client's/tenant's trip is blocked")
        void employeeCannotSubscribeToOtherTenantTrip() {
            assertThatThrownBy(() -> authService.authorizeSubscription(emp1, "/topic/trip/TRIP-201"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Tenant isolation violation");
        }

        @Test
        @DisplayName("Employee attempting to subscribe to an office topic is blocked")
        void employeeCannotSubscribeToOffice() {
            assertThatThrownBy(() -> authService.authorizeSubscription(emp1, "/topic/office/OFFICE-BLR"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Employees are only permitted to subscribe to trips");
        }

        @Test
        @DisplayName("Employee attempting to subscribe to a cab topic is blocked")
        void employeeCannotSubscribeToCab() {
            assertThatThrownBy(() -> authService.authorizeSubscription(emp1, "/topic/cab/CAB-01"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Employees are only permitted to subscribe to trips");
        }
    }

    @Nested
    @DisplayName("Office Admin Subscription Rules")
    class OfficeAdminRules {

        private final UserPrincipal adminBlr = UserPrincipal.builder()
                .userId("admin-blr")
                .tenantId("TENANT-A")
                .role(UserRole.OFFICE_ADMIN)
                .assignedOfficeId("OFFICE-BLR")
                .build();

        @Test
        @DisplayName("Office Admin can subscribe to their assigned office")
        void adminCanSubscribeToAssignedOffice() {
            assertThatCode(() -> authService.authorizeSubscription(adminBlr, "/topic/office/OFFICE-BLR"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Office Admin can subscribe to trips belonging to their assigned office")
        void adminCanSubscribeToTripsInTheirOffice() {
            // TRIP-101 is located in OFFICE-BLR
            assertThatCode(() -> authService.authorizeSubscription(adminBlr, "/topic/trip/TRIP-101"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Office Admin attempting to subscribe to another office in same tenant is blocked")
        void adminCannotSubscribeToOtherOfficeInSameTenant() {
            assertThatThrownBy(() -> authService.authorizeSubscription(adminBlr, "/topic/office/OFFICE-HYD"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Office admins can only subscribe to their assigned office");
        }

        @Test
        @DisplayName("Office Admin attempting to subscribe to another client's office in different tenant is blocked")
        void adminCannotSubscribeToOtherTenantOffice() {
            assertThatThrownBy(() -> authService.authorizeSubscription(adminBlr, "/topic/office/OFFICE-NYC"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Office admins can only subscribe to their assigned office");
        }

        @Test
        @DisplayName("Office Admin attempting to subscribe to a trip in another office is blocked")
        void adminCannotSubscribeToTripInOtherOffice() {
            // TRIP-103 is in OFFICE-HYD
            assertThatThrownBy(() -> authService.authorizeSubscription(adminBlr, "/topic/trip/TRIP-103"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Office admin can only subscribe to trips within office: OFFICE-BLR");
        }
    }

    @Nested
    @DisplayName("Security Desk & Operator Subscription Rules")
    class SecurityDeskRules {

        private final UserPrincipal securityTenantA = UserPrincipal.builder()
                .userId("sec-desk-1")
                .tenantId("TENANT-A")
                .role(UserRole.SECURITY_DESK)
                .build();

        @Test
        @DisplayName("Security desk can subscribe to any trip, office, or cab within their tenant")
        void securityDeskCanSubscribeWithinTenant() {
            assertThatCode(() -> authService.authorizeSubscription(securityTenantA, "/topic/trip/TRIP-101"))
                    .doesNotThrowAnyException();
            assertThatCode(() -> authService.authorizeSubscription(securityTenantA, "/topic/office/OFFICE-BLR"))
                    .doesNotThrowAnyException();
            assertThatCode(() -> authService.authorizeSubscription(securityTenantA, "/topic/cab/CAB-01"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Security desk attempting to subscribe to another client's trip is blocked")
        void securityDeskCannotAccessOtherTenantTrip() {
            assertThatThrownBy(() -> authService.authorizeSubscription(securityTenantA, "/topic/trip/TRIP-201"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Tenant isolation violation");
        }

        @Test
        @DisplayName("Security desk attempting to subscribe to another client's office is blocked")
        void securityDeskCannotAccessOtherTenantOffice() {
            assertThatThrownBy(() -> authService.authorizeSubscription(securityTenantA, "/topic/office/OFFICE-NYC"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Tenant isolation violation");
        }

        @Test
        @DisplayName("Security desk attempting to subscribe to another client's cab is blocked")
        void securityDeskCannotAccessOtherTenantCab() {
            assertThatThrownBy(() -> authService.authorizeSubscription(securityTenantA, "/topic/cab/CAB-99"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Tenant isolation violation");
        }
    }

    @Nested
    @DisplayName("Unauthenticated & Anonymous Access")
    class UnauthenticatedRules {

        @Test
        @DisplayName("Unauthenticated subscription attempts are unconditionally blocked")
        void unauthenticatedUserBlocked() {
            assertThatThrownBy(() -> authService.authorizeSubscription(null, "/topic/trip/TRIP-101"))
                    .isInstanceOf(SubscriptionSecurityException.class)
                    .hasMessageContaining("Unauthenticated");
        }
    }
}

