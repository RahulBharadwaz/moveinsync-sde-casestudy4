package com.moveinsync.tripfanout.engine;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.LocationEvent;
import com.moveinsync.tripfanout.model.SOSEvent;
import com.moveinsync.tripfanout.model.StatusEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressWarnings("null")
class ClientSessionQueueTest {

    @Test
    @DisplayName("Priority Lane: SOS alert skips ahead of all pending location and status traffic")
    void testPriorityLaneOvertaking() {
        ClientSessionQueue queue = new ClientSessionQueue("sess-1", 50);

        // 1. Enqueue 5 location telemetry updates
        for (int i = 1; i <= 5; i++) {
            queue.enqueue(LocationEvent.builder()
                    .eventId("loc-" + i)
                    .cabId("CAB-" + i)
                    .sequenceNumber((long) i)
                    .latitude(12.0 + i)
                    .build());
        }

        // 2. Enqueue 2 status updates
        queue.enqueue(StatusEvent.builder()
                .eventId("stat-1")
                .sequenceNumber(6L)
                .status("CAB_ARRIVED")
                .build());

        queue.enqueue(StatusEvent.builder()
                .eventId("stat-2")
                .sequenceNumber(7L)
                .status("TRIP_STARTED")
                .build());

        // 3. Enqueue emergency SOS alert (arrived LAST!)
        queue.enqueue(SOSEvent.builder()
                .eventId("sos-1")
                .sequenceNumber(8L)
                .alertType("PANIC_BUTTON")
                .severity("CRITICAL")
                .build());

        // 4. Poll events and verify delivery order
        List<Event> delivered = new ArrayList<>();
        Event e;
        while ((e = queue.poll()) != null) {
            delivered.add(e);
        }

        assertThat(delivered).hasSize(8);

        // First delivered MUST be the SOS event (Priority 0 lane)
        assertThat(delivered.get(0)).isInstanceOf(SOSEvent.class);
        assertThat(((SOSEvent) delivered.get(0)).getAlertType()).isEqualTo("PANIC_BUTTON");

        // Second and third MUST be the Status events in arrival sequence (Priority 1 lane)
        assertThat(delivered.get(1)).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) delivered.get(1)).getStatus()).isEqualTo("CAB_ARRIVED");

        assertThat(delivered.get(2)).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) delivered.get(2)).getStatus()).isEqualTo("TRIP_STARTED");

        // Remaining 5 events are the Location events (Priority 2 lane)
        for (int i = 3; i < 8; i++) {
            assertThat(delivered.get(i)).isInstanceOf(LocationEvent.class);
        }
    }

    @Test
    @DisplayName("Conflation: Location updates for the same cab are conflated to keep only the latest update")
    void testConflationPerCab() {
        ClientSessionQueue queue = new ClientSessionQueue("sess-conflate", 50);

        // Enqueue 10 successive location updates for CAB-101
        for (int i = 1; i <= 10; i++) {
            queue.enqueue(LocationEvent.builder()
                    .eventId("loc-101-" + i)
                    .cabId("CAB-101")
                    .sequenceNumber((long) i)
                    .latitude(12.0 + (i * 0.1))
                    .longitude(77.0 + (i * 0.1))
                    .build());
        }

        // Enqueue 5 successive location updates for CAB-102
        for (int i = 1; i <= 5; i++) {
            queue.enqueue(LocationEvent.builder()
                    .eventId("loc-102-" + i)
                    .cabId("CAB-102")
                    .sequenceNumber((long) (10 + i))
                    .latitude(13.0 + (i * 0.1))
                    .longitude(78.0 + (i * 0.1))
                    .build());
        }

        // Conflation metrics: 9 conflated for CAB-101 + 4 conflated for CAB-102 = 13 total conflated
        assertThat(queue.getConflatedCount().get()).isEqualTo(13L);

        // Queue size should be exactly 2 (one slot per cab!)
        assertThat(queue.size()).isEqualTo(2);

        // Poll and verify only the latest location per cab is delivered
        Event first = queue.poll();
        assertNotNull(first);
        assertThat(first).isInstanceOf(LocationEvent.class);
        LocationEvent loc1 = (LocationEvent) first;
        assertNotNull(loc1);
        assertThat(loc1.getCabId()).isEqualTo("CAB-101");
        assertThat(loc1.getLatitude()).isEqualTo(13.0); // 12.0 + (10 * 0.1) = 13.0
        assertThat(loc1.getSequenceNumber()).isEqualTo(10L);

        Event second = queue.poll();
        assertNotNull(second);
        assertThat(second).isInstanceOf(LocationEvent.class);
        LocationEvent loc2 = (LocationEvent) second;
        assertNotNull(loc2);
        assertThat(loc2.getCabId()).isEqualTo("CAB-102");
        assertThat(loc2.getLatitude()).isEqualTo(13.5); // 13.0 + (5 * 0.1) = 13.5
        assertThat(loc2.getSequenceNumber()).isEqualTo(15L);

        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Back-Pressure Invariance: Status changes and SOS events are NEVER dropped even when queue overflows")
    void testStatusAndSOSNeverDroppedUnderBackPressure() {
        int smallCapacity = 5;
        ClientSessionQueue queue = new ClientSessionQueue("sess-overflow", smallCapacity);

        // Enqueue 5 Status events
        for (int i = 1; i <= 5; i++) {
            queue.enqueue(StatusEvent.builder()
                    .eventId("stat-" + i)
                    .sequenceNumber((long) i)
                    .status("STATUS_" + i)
                    .build());
        }

        // Enqueue 20 location updates across 10 different cabs
        for (int i = 1; i <= 20; i++) {
            queue.enqueue(LocationEvent.builder()
                    .eventId("loc-" + i)
                    .cabId("CAB-" + (i % 10))
                    .sequenceNumber((long) (100 + i))
                    .build());
        }

        // Enqueue 2 SOS emergency events
        queue.enqueue(SOSEvent.builder()
                .eventId("sos-1")
                .sequenceNumber(301L)
                .alertType("PANIC_BUTTON")
                .build());

        queue.enqueue(SOSEvent.builder()
                .eventId("sos-2")
                .sequenceNumber(302L)
                .alertType("ROUTE_DEVIATION")
                .build());

        // Drain all events
        List<Event> polled = new ArrayList<>();
        Event e;
        while ((e = queue.poll()) != null) {
            polled.add(e);
        }

        // Verify SOS events: Both present and at the front!
        List<SOSEvent> sosList = polled.stream()
                .filter(ev -> ev instanceof SOSEvent)
                .map(ev -> (SOSEvent) ev)
                .toList();
        assertThat(sosList).hasSize(2);
        assertNotNull(sosList.get(0));
        assertNotNull(sosList.get(1));
        assertThat(sosList.get(0).getEventId()).isEqualTo("sos-1");
        assertThat(sosList.get(1).getEventId()).isEqualTo("sos-2");

        // Verify Status events: All 5 present in sequence! ZERO dropped!
        List<StatusEvent> statusList = polled.stream()
                .filter(ev -> ev instanceof StatusEvent)
                .map(ev -> (StatusEvent) ev)
                .toList();
        assertThat(statusList).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertNotNull(statusList.get(i));
            assertThat(statusList.get(i).getStatus()).isEqualTo("STATUS_" + (i + 1));
        }

        // Location updates were bounded/dropped to protect memory
        assertThat(queue.getDroppedLocationCount().get()).isGreaterThan(0L);
    }
}

