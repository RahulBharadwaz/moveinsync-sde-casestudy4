package com.moveinsync.tripfanout.engine;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.LocationEvent;
import com.moveinsync.tripfanout.model.SOSEvent;
import com.moveinsync.tripfanout.model.StatusEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("null")
class SlowConsumerSimulationTest {

    private GatewayQueueManager queueManager;
    private SlowConsumerPolicy policy;

    @BeforeEach
    void setUp() {
        policy = SlowConsumerPolicy.builder()
                .maxCapacity(30)
                .conflationEnabled(true)
                .build();
        queueManager = new GatewayQueueManager(policy);
    }

    @Test
    @DisplayName("Case Study Simulation: Slow/Frozen security desk never slows down other desks, conflates locations, and receives SOS immediately upon unfreezing")
    void testSlowConsumerSimulationWithConflationAndPriorityLane() throws Exception {
        String topic = "office:OFFICE-BLR";
        String slowSessionId = "security-desk-frozen";
        String fastSessionId = "security-desk-normal";

        // Register both security desks to the office topic
        queueManager.registerSubscription(slowSessionId, topic);
        queueManager.registerSubscription(fastSessionId, topic);

        ClientSessionQueue slowQueue = queueManager.getQueue(slowSessionId).orElseThrow();
        ClientSessionQueue fastQueue = queueManager.getQueue(fastSessionId).orElseThrow();

        // 1. Asynchronously simulate fast consumer draining in real time (< 1ms)
        List<Event> fastConsumerReceived = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean fastConsumerRunning = new AtomicBoolean(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> fastConsumerTask = executor.submit(() -> {
            while (fastConsumerRunning.get()) {
                Event e = fastQueue.poll();
                if (e != null) {
                    fastConsumerReceived.add(e);
                } else {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }
        });

        // 2. Slow consumer does NOT drain (browser frozen / bad network stall)

        // 3. Ingestion thread pushes high-frequency traffic to the topic:
        //    - 40 Location events for 4 cabs (10 updates each)
        //    - 3 Status events
        //    - 1 Emergency SOS event
        long startTime = System.currentTimeMillis();

        // Push 40 location updates (10 per cab)
        for (int i = 1; i <= 10; i++) {
            for (int cab = 1; cab <= 4; cab++) {
                queueManager.fanout(LocationEvent.builder()
                        .eventId("loc-cab" + cab + "-seq" + i)
                        .topic(topic)
                        .cabId("CAB-" + cab)
                        .sequenceNumber((long) (i * 10 + cab))
                        .latitude(12.0 + (i * 0.1))
                        .longitude(77.0 + (i * 0.1))
                        .build());
            }
        }

        // Push 3 Status events
        queueManager.fanout(StatusEvent.builder()
                .eventId("stat-1")
                .topic(topic)
                .sequenceNumber(201L)
                .status("DRIVER_ASSIGNED")
                .build());

        queueManager.fanout(StatusEvent.builder()
                .eventId("stat-2")
                .topic(topic)
                .sequenceNumber(202L)
                .status("CAB_ARRIVED")
                .build());

        queueManager.fanout(StatusEvent.builder()
                .eventId("stat-3")
                .topic(topic)
                .sequenceNumber(203L)
                .status("TRIP_STARTED")
                .build());

        // Push 1 Emergency SOS event (Priority 0)
        queueManager.fanout(SOSEvent.builder()
                .eventId("sos-emergency-1")
                .topic(topic)
                .sequenceNumber(301L)
                .alertType("PANIC_BUTTON")
                .severity("CRITICAL")
                .message("Passenger pressed panic button")
                .build());

        long producerDuration = System.currentTimeMillis() - startTime;

        // GUARANTEE 1: Ingestion / Gateway thread was never blocked by the slow consumer
        assertThat(producerDuration).isLessThan(200); // 44 fanouts completed almost instantaneously

        // Stop fast consumer and verify it received the events
        Thread.sleep(50);
        fastConsumerRunning.set(false);
        fastConsumerTask.get(2, TimeUnit.SECONDS);

        // Drain any remaining in fastQueue
        Event remainingFast;
        while ((remainingFast = fastQueue.poll()) != null) {
            fastConsumerReceived.add(remainingFast);
        }

        assertThat(fastConsumerReceived).isNotEmpty();
        assertThat(fastConsumerReceived.stream().anyMatch(e -> e instanceof SOSEvent)).isTrue();

        // GUARANTEE 2: Inspect slow consumer's queue before unfreezing
        // Location updates were conflated in place: 40 incoming locations conflated down to 4 (1 per cab)
        assertThat(slowQueue.getConflatedCount().get()).isEqualTo(36L); // 40 - 4 = 36 conflations

        // Queue contains: 1 SOS + 3 Status + 4 Conflated Locations = 8 events total
        assertThat(slowQueue.size()).isEqualTo(8);

        // GUARANTEE 3: Slow consumer unfreezes and drains its queue
        List<Event> slowConsumerDelivered = new ArrayList<>();
        Event nextEvent;
        while ((nextEvent = slowQueue.poll()) != null) {
            slowConsumerDelivered.add(nextEvent);
        }

        assertThat(slowConsumerDelivered).hasSize(8);

        // FIRST event delivered MUST be the SOS alert! (Priority Lane overtakes all location traffic)
        assertThat(slowConsumerDelivered.get(0)).isInstanceOf(SOSEvent.class);
        SOSEvent deliveredSos = (SOSEvent) slowConsumerDelivered.get(0);
        assertThat(deliveredSos.getEventId()).isEqualTo("sos-emergency-1");
        assertThat(deliveredSos.getAlertType()).isEqualTo("PANIC_BUTTON");

        // NEXT 3 events delivered MUST be the 3 Status events in strict sequence
        assertThat(slowConsumerDelivered.get(1)).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) slowConsumerDelivered.get(1)).getStatus()).isEqualTo("DRIVER_ASSIGNED");

        assertThat(slowConsumerDelivered.get(2)).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) slowConsumerDelivered.get(2)).getStatus()).isEqualTo("CAB_ARRIVED");

        assertThat(slowConsumerDelivered.get(3)).isInstanceOf(StatusEvent.class);
        assertThat(((StatusEvent) slowConsumerDelivered.get(3)).getStatus()).isEqualTo("TRIP_STARTED");

        // REMAINING 4 events are the latest conflated locations for each cab
        for (int i = 4; i < 8; i++) {
            assertThat(slowConsumerDelivered.get(i)).isInstanceOf(LocationEvent.class);
            LocationEvent loc = (LocationEvent) slowConsumerDelivered.get(i);
            // Verify it received the 10th (latest) update, latitude = 13.0 (12.0 + 1.0)
            assertThat(loc.getLatitude()).isEqualTo(13.0);
        }

        executor.shutdownNow();
    }
}
