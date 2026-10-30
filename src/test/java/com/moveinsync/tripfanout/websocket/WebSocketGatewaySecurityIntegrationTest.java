package com.moveinsync.tripfanout.websocket;

import com.moveinsync.tripfanout.model.LocationEvent;
import com.moveinsync.tripfanout.security.SubscriptionSecurityException;
import com.moveinsync.tripfanout.security.UserPrincipal;
import com.moveinsync.tripfanout.security.UserRole;
import com.moveinsync.tripfanout.security.WebSocketSecurityInterceptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest
class WebSocketGatewaySecurityIntegrationTest {

    @Autowired
    private WebSocketSecurityInterceptor securityInterceptor;

    @Autowired
    private RedisToWebSocketBridge webSocketBridge;

    @MockitoBean
    private SimpMessagingTemplate messagingTemplate;

    @Test
    @DisplayName("STOMP Pipeline: authenticates on CONNECT and allows authorized SUBSCRIBE to own trip")
    void testConnectAndAuthorizedSubscription() {
        // 1. Simulate CONNECT with token
        StompHeaderAccessor connectAccessor = StompHeaderAccessor.create(StompCommand.CONNECT);
        connectAccessor.setNativeHeader("Authorization", "Bearer token-emp-1");
        connectAccessor.setSessionId("sess-001");
        connectAccessor.setLeaveMutable(true);
        Message<byte[]> connectMsg = MessageBuilder.createMessage(new byte[0], connectAccessor.getMessageHeaders());

        Message<?> processedConnect = securityInterceptor.preSend(connectMsg, null);
        StompHeaderAccessor resultConnect = StompHeaderAccessor.wrap(processedConnect);

        assertThat(resultConnect.getUser()).isNotNull();
        assertThat(resultConnect.getUser()).isInstanceOf(UserPrincipal.class);
        UserPrincipal user = (UserPrincipal) resultConnect.getUser();
        assertThat(user.getUserId()).isEqualTo("emp-1");
        assertThat(user.getRole()).isEqualTo(UserRole.EMPLOYEE);
        assertThat(user.getTenantId()).isEqualTo("TENANT-A");

        // 2. Simulate SUBSCRIBE to own trip
        StompHeaderAccessor subAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        subAccessor.setUser(user);
        subAccessor.setDestination("/topic/trip/TRIP-101");
        subAccessor.setSubscriptionId("sub-1");
        subAccessor.setLeaveMutable(true);
        Message<byte[]> subMsg = MessageBuilder.createMessage(new byte[0], subAccessor.getMessageHeaders());

        Message<?> processedSub = securityInterceptor.preSend(subMsg, null);
        assertThat(processedSub).isNotNull();
    }

    @Test
    @DisplayName("Tenant Isolation: Employee subscribing to another employee's trip in same tenant is rejected")
    void testEmployeeCannotSubscribeToAnotherEmployeeTrip() {
        UserPrincipal emp1 = UserPrincipal.builder()
                .userId("emp-1")
                .tenantId("TENANT-A")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-101")
                .build();

        StompHeaderAccessor subAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        subAccessor.setUser(emp1);
        subAccessor.setDestination("/topic/trip/TRIP-102"); // Belongs to emp-2!
        subAccessor.setSubscriptionId("sub-illegal");
        subAccessor.setLeaveMutable(true);
        Message<byte[]> subMsg = MessageBuilder.createMessage(new byte[0], subAccessor.getMessageHeaders());

        assertThatThrownBy(() -> securityInterceptor.preSend(subMsg, null))
                .isInstanceOf(SubscriptionSecurityException.class)
                .hasMessageContaining("Employees can only subscribe to their own trip");
    }

    @Test
    @DisplayName("Tenant Isolation: Employee subscribing to another tenant's trip is strictly rejected")
    void testEmployeeCannotSubscribeToOtherTenantTrip() {
        UserPrincipal emp1 = UserPrincipal.builder()
                .userId("emp-1")
                .tenantId("TENANT-A")
                .role(UserRole.EMPLOYEE)
                .assignedTripId("TRIP-101")
                .build();

        StompHeaderAccessor subAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        subAccessor.setUser(emp1);
        subAccessor.setDestination("/topic/trip/TRIP-201"); // Belongs to TENANT-B!
        subAccessor.setSubscriptionId("sub-breach");
        subAccessor.setLeaveMutable(true);
        Message<byte[]> subMsg = MessageBuilder.createMessage(new byte[0], subAccessor.getMessageHeaders());

        assertThatThrownBy(() -> securityInterceptor.preSend(subMsg, null))
                .isInstanceOf(SubscriptionSecurityException.class)
                .hasMessageContaining("Tenant isolation violation");
    }

    @Test
    @DisplayName("Tenant Isolation: Office admin subscribing to another tenant's office is rejected")
    void testAdminCannotSubscribeToOtherTenantOffice() {
        UserPrincipal adminBlr = UserPrincipal.builder()
                .userId("admin-blr")
                .tenantId("TENANT-A")
                .role(UserRole.OFFICE_ADMIN)
                .assignedOfficeId("OFFICE-BLR")
                .build();

        StompHeaderAccessor subAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        subAccessor.setUser(adminBlr);
        subAccessor.setDestination("/topic/office/OFFICE-NYC"); // Belongs to TENANT-B!
        subAccessor.setSubscriptionId("sub-admin-breach");
        subAccessor.setLeaveMutable(true);
        Message<byte[]> subMsg = MessageBuilder.createMessage(new byte[0], subAccessor.getMessageHeaders());

        assertThatThrownBy(() -> securityInterceptor.preSend(subMsg, null))
                .isInstanceOf(SubscriptionSecurityException.class)
                .hasMessageContaining("Office admins can only subscribe to their assigned office");
    }

    @Test
    @DisplayName("The Bridge: events from Redis are forwarded to the correct WebSocket topic destination")
    void testRedisToWebSocketBridgeRouting() {
        LocationEvent locEvent = LocationEvent.builder()
                .eventId("bridge-evt-1")
                .topic("trip:TRIP-101")
                .sequenceNumber(42L)
                .cabId("CAB-01")
                .latitude(12.9716)
                .longitude(77.5946)
                .build();

        webSocketBridge.routeToWebSocket(locEvent);

        // Verify that SimpMessagingTemplate forwarded the event to "/topic/trip/TRIP-101"
        ArgumentCaptor<LocationEvent> captor = ArgumentCaptor.forClass(LocationEvent.class);
        verify(messagingTemplate).convertAndSend(eq("/topic/trip/TRIP-101"), captor.capture());

        LocationEvent forwarded = captor.getValue();
        assertThat(forwarded.getEventId()).isEqualTo("bridge-evt-1");
        assertThat(forwarded.getSequenceNumber()).isEqualTo(42L);
        assertThat(forwarded.getLatitude()).isEqualTo(12.9716);
    }
}
