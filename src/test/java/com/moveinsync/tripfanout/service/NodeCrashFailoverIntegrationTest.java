package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.engine.ClientSessionQueue;
import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import com.moveinsync.tripfanout.engine.SlowConsumerPolicy;
import com.moveinsync.tripfanout.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Advanced Track: System Failure Injection Test.
 * Simulates sudden Gateway Node death during an active trip.
 * Validates that when a client reconnects to a surviving gateway node,
 * StateHydrationService accurately recovers all missed events from the Redis bounded buffer
 * with ZERO loss of Status or SOS events, preserving strict sequence ordering.
 */
@SpringBootTest
class NodeCrashFailoverIntegrationTest {

    @Autowired
    private EventSequencingService sequencingService;

    @Autowired
    private StateHydrationService hydrationService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String CRASH_TRIP_TOPIC = "trip:TRIP-CRASH-RECOVERY";

    @BeforeEach
    @AfterEach
    void cleanUpTopic() {
        redisTemplate.delete(EventSequencingService.getSequenceKey(CRASH_TRIP_TOPIC));
        redisTemplate.delete(EventSequencingService.getBufferKey(CRASH_TRIP_TOPIC));
        redisTemplate.delete(EventSequencingService.getSnapshotKey(CRASH_TRIP_TOPIC));
    }

    @Test
    @DisplayName("System Failure Injection: Gateway Node 1 crashes; client reconnects to Node 2 without losing Status or SOS events")
    void testGatewayNodeCrashAndFailoverRecovery() {
        SlowConsumerPolicy policy = SlowConsumerPolicy.builder().maxCapacity(50).build();

        // Gateway Node 1 (Initial connection point)
        GatewayQueueManager gatewayNode1 = new GatewayQueueManager(policy);
        String clientSessionNode1 = "client-screen-session-on-node-1";
        gatewayNode1.registerSubscription(clientSessionNode1, CRASH_TRIP_TOPIC);
        assertThat(gatewayNode1.getQueue(clientSessionNode1)).isPresent();

        // --- STEP 1: Normal Ingestion while Node 1 is Active ---
        Event e1 = sequencingService.ingest(StatusEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .tripId("TRIP-CRASH-RECOVERY")
                .status("DRIVER_ASSIGNED")
                .build());

        Event e2 = sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9100)
                .longitude(77.5100)
                .build());

        Event e3 = sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9150)
                .longitude(77.5150)
                .build());

        // Node 1 dispatches to client
        gatewayNode1.fanout(e1);
        gatewayNode1.fanout(e2);
        gatewayNode1.fanout(e3);

        // Client on Node 1 consumes up to sequence 3
        assertThat(e1.getSequenceNumber()).isEqualTo(1L);
        assertThat(e2.getSequenceNumber()).isEqualTo(2L);
        assertThat(e3.getSequenceNumber()).isEqualTo(3L);

        long lastReceivedSeqOnClient = 3L;

        // --- STEP 2: INJECT FAILURE - Gateway Node 1 Crashes ---
        // Node 1 memory is wiped, sockets terminated abruptly
        gatewayNode1.removeSession(clientSessionNode1);
        assertThat(gatewayNode1.getActiveSessionCount()).isEqualTo(0);

        // --- STEP 3: Active Trip updates continue into cluster while client is disconnected ---
        // Event 4: Location update
        sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9200)
                .longitude(77.5200)
                .build());

        // Event 5: Critical Status update
        sequencingService.ingest(StatusEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .tripId("TRIP-CRASH-RECOVERY")
                .status("DELAYED_DUE_TO_TRAFFIC")
                .delayMinutes(15)
                .build());

        // Event 6: Location update
        sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9250)
                .longitude(77.5250)
                .build());

        // Event 7: Emergency SOS Alert! (Critical security event)
        sequencingService.ingest(SOSEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .tripId("TRIP-CRASH-RECOVERY")
                .cabId("CAB-CRASH-1")
                .employeeId("emp-passenger-1")
                .alertType("PANIC_BUTTON")
                .severity("CRITICAL")
                .message("Passenger triggered panic button")
                .build());

        // Event 8: Status update
        sequencingService.ingest(StatusEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .tripId("TRIP-CRASH-RECOVERY")
                .status("CAB_ARRIVED")
                .build());

        // Event 9: Location update
        Event e9 = sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9300)
                .longitude(77.5300)
                .build());

        assertThat(e9.getSequenceNumber()).isEqualTo(9L);

        // --- STEP 4: Client reconnects to Surviving Gateway Node 2 ---
        GatewayQueueManager gatewayNode2 = new GatewayQueueManager(policy);
        String clientSessionNode2 = "client-screen-failover-on-node-2";
        gatewayNode2.registerSubscription(clientSessionNode2, CRASH_TRIP_TOPIC);
        ClientSessionQueue queueNode2 = gatewayNode2.getQueue(clientSessionNode2).orElseThrow();

        // Client initiates state hydration with its last sequence number (3)
        HydrationResult hydration = hydrationService.hydrate(CRASH_TRIP_TOPIC, lastReceivedSeqOnClient);

        // --- STEP 5: Verify Flawless Resync ---
        assertThat(hydration.getType()).isEqualTo(HydrationResult.HydrationType.REPLAY);
        assertThat(hydration.getRequestedSequenceNumber()).isEqualTo(3L);
        assertThat(hydration.getCurrentSequenceNumber()).isEqualTo(9L);

        List<Event> recoveredEvents = hydration.getMissedEvents();
        assertThat(recoveredEvents).hasSize(6);

        // 1. Verify exact strict ordering: 4, 5, 6, 7, 8, 9
        List<Long> recoveredSeqList = recoveredEvents.stream().map(Event::getSequenceNumber).toList();
        assertThat(recoveredSeqList).containsExactly(4L, 5L, 6L, 7L, 8L, 9L);

        // 2. Verify Status Events were completely preserved (ZERO LOSS)
        List<StatusEvent> recoveredStatusList = recoveredEvents.stream()
                .filter(ev -> ev instanceof StatusEvent)
                .map(ev -> (StatusEvent) ev)
                .toList();
        assertThat(recoveredStatusList).hasSize(2);
        assertThat(recoveredStatusList.get(0).getStatus()).isEqualTo("DELAYED_DUE_TO_TRAFFIC");
        assertThat(recoveredStatusList.get(1).getStatus()).isEqualTo("CAB_ARRIVED");

        // 3. Verify SOS Event was completely preserved (ZERO LOSS)
        List<SOSEvent> recoveredSosList = recoveredEvents.stream()
                .filter(ev -> ev instanceof SOSEvent)
                .map(ev -> (SOSEvent) ev)
                .toList();
        assertThat(recoveredSosList).hasSize(1);
        SOSEvent recoveredSos = recoveredSosList.get(0);
        assertThat(recoveredSos.getSequenceNumber()).isEqualTo(7L);
        assertThat(recoveredSos.getAlertType()).isEqualTo("PANIC_BUTTON");
        assertThat(recoveredSos.getSeverity()).isEqualTo("CRITICAL");

        // --- STEP 6: Enqueue recovered events into Node 2 queue and verify Live traffic continues ---
        for (Event recovered : recoveredEvents) {
            queueNode2.enqueue(recovered);
        }

        // Live Event 10 arrives on Node 2
        Event e10 = sequencingService.ingest(LocationEvent.builder()
                .topic(CRASH_TRIP_TOPIC)
                .cabId("CAB-CRASH-1")
                .latitude(12.9350)
                .longitude(77.5350)
                .build());
        gatewayNode2.fanout(e10);

        // When draining queue on Node 2:
        // SOS alert (Priority 0) is delivered first!
        Event firstDelivered = queueNode2.poll();
        assertThat(firstDelivered).isInstanceOf(SOSEvent.class);
        assertThat(((SOSEvent) firstDelivered).getAlertType()).isEqualTo("PANIC_BUTTON");

        // Status events are delivered next in sequence order
        Event secondDelivered = queueNode2.poll();
        assertThat(secondDelivered).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) secondDelivered).getStatus()).isEqualTo("DELAYED_DUE_TO_TRAFFIC");

        Event thirdDelivered = queueNode2.poll();
        assertThat(thirdDelivered).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) thirdDelivered).getStatus()).isEqualTo("CAB_ARRIVED");
    }
}

