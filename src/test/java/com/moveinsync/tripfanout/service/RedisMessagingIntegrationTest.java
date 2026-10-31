package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@SuppressWarnings("null")
class RedisMessagingIntegrationTest {

    @Autowired
    private EventSequencingService sequencingService;

    @Autowired
    private StateHydrationService hydrationService;

    @Autowired
    private RedisEventSubscriber eventSubscriber;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String TEST_TOPIC_1 = "test-trip-101";
    private static final String TEST_TOPIC_2 = "test-trip-102";
    private static final String TEST_TOPIC_EVICTION = "test-trip-eviction";

    @BeforeEach
    @AfterEach
    void cleanUpRedis() {
        cleanTopic(TEST_TOPIC_1);
        cleanTopic(TEST_TOPIC_2);
        cleanTopic(TEST_TOPIC_EVICTION);
    }

    private void cleanTopic(String topic) {
        redisTemplate.delete(EventSequencingService.getSequenceKey(topic));
        redisTemplate.delete(EventSequencingService.getBufferKey(topic));
        redisTemplate.delete(EventSequencingService.getSnapshotKey(topic));
    }

    @Test
    @DisplayName("Strict Sequencing: monotonically increments sequence number per topic with complete isolation")
    void testStrictMonotonicSequenceIncrement() {
        // Ingest on Topic 1
        Event e1 = sequencingService.ingest(LocationEvent.builder()
                .topic(TEST_TOPIC_1)
                .cabId("CAB-1")
                .latitude(12.97)
                .longitude(77.59)
                .build());

        Event e2 = sequencingService.ingest(StatusEvent.builder()
                .topic(TEST_TOPIC_1)
                .status("DRIVER_ASSIGNED")
                .build());

        Event e3 = sequencingService.ingest(SOSEvent.builder()
                .topic(TEST_TOPIC_1)
                .alertType("PANIC_BUTTON")
                .build());

        assertThat(e1.getSequenceNumber()).isEqualTo(1L);
        assertThat(e2.getSequenceNumber()).isEqualTo(2L);
        assertThat(e3.getSequenceNumber()).isEqualTo(3L);
        assertThat(sequencingService.getCurrentSequenceNumber(TEST_TOPIC_1)).isEqualTo(3L);

        // Topic 2 isolation: sequence starts at 1 independently
        Event eTopic2 = sequencingService.ingest(LocationEvent.builder()
                .topic(TEST_TOPIC_2)
                .cabId("CAB-2")
                .build());

        assertThat(eTopic2.getSequenceNumber()).isEqualTo(1L);
        assertThat(sequencingService.getCurrentSequenceNumber(TEST_TOPIC_2)).isEqualTo(1L);
        assertThat(sequencingService.getCurrentSequenceNumber(TEST_TOPIC_1)).isEqualTo(3L);
    }

    @Test
    @DisplayName("Bounded Buffer: maintains maxBufferSize and evicts older messages when capacity is exceeded")
    void testBoundedBufferEviction() {
        int originalCapacity = sequencingService.getMaxBufferSize();
        int testCapacity = 5;
        sequencingService.setMaxBufferSize(testCapacity);

        try {
            // Ingest 12 events into a buffer with capacity 5
            for (int i = 1; i <= 12; i++) {
                sequencingService.ingest(LocationEvent.builder()
                        .topic(TEST_TOPIC_EVICTION)
                        .cabId("CAB-EVIC")
                        .latitude(12.0 + i)
                        .longitude(77.0 + i)
                        .build());
            }

            // Buffer size must be capped at 5
            long bufferSize = sequencingService.getBufferSize(TEST_TOPIC_EVICTION);
            assertThat(bufferSize).isEqualTo(testCapacity);

            // Total sequence count is 12
            assertThat(sequencingService.getCurrentSequenceNumber(TEST_TOPIC_EVICTION)).isEqualTo(12L);

            // Replay requesting from seq 7 should fetch 8, 9, 10, 11, 12
            HydrationResult result = hydrationService.hydrate(TEST_TOPIC_EVICTION, 7L);
            assertThat(result.getType()).isEqualTo(HydrationResult.HydrationType.REPLAY);
            assertThat(result.getMissedEvents()).hasSize(5);
            assertThat(result.getMissedEvents().stream().map(Event::getSequenceNumber).toList())
                    .containsExactly(8L, 9L, 10L, 11L, 12L);

        } finally {
            sequencingService.setMaxBufferSize(originalCapacity);
        }
    }

    @Test
    @DisplayName("State Hydration: returns sequential missed events when client sequence is within buffer")
    void testResumableStateHydrationReplay() {
        for (int i = 1; i <= 10; i++) {
            sequencingService.ingest(LocationEvent.builder()
                    .topic(TEST_TOPIC_1)
                    .cabId("CAB-1")
                    .latitude(12.0 + (i * 0.01))
                    .longitude(77.0 + (i * 0.01))
                    .build());
        }

        // Client reconnects having received up to sequence 6
        HydrationResult result = hydrationService.hydrate(TEST_TOPIC_1, 6L);

        assertThat(result.getType()).isEqualTo(HydrationResult.HydrationType.REPLAY);
        assertThat(result.getRequestedSequenceNumber()).isEqualTo(6L);
        assertThat(result.getCurrentSequenceNumber()).isEqualTo(10L);
        assertThat(result.getMissedEvents()).hasSize(4);

        // Verify strictly ordered sequential delivery
        List<Long> sequenceNumbers = result.getMissedEvents().stream()
                .map(Event::getSequenceNumber)
                .toList();
        assertThat(sequenceNumbers).containsExactly(7L, 8L, 9L, 10L);

        // Verify event subclass and fields preserved
        Event firstMissed = result.getMissedEvents().get(0);
        assertThat(firstMissed).isInstanceOf(LocationEvent.class);
        LocationEvent loc = (LocationEvent) firstMissed;
        assertThat(loc.getLatitude()).isEqualTo(12.07);
    }

    @Test
    @DisplayName("State Hydration: returns full current snapshot when client sequence has been evicted")
    void testResumableStateHydrationEvictedSnapshot() {
        int originalCapacity = sequencingService.getMaxBufferSize();
        int testCapacity = 4;
        sequencingService.setMaxBufferSize(testCapacity);

        try {
            // Event 1: Initial status
            sequencingService.ingest(StatusEvent.builder()
                    .topic(TEST_TOPIC_EVICTION)
                    .tripId("TRIP-EVIC")
                    .status("TRIP_STARTED")
                    .build());

            // Events 2 - 9: Location updates
            for (int i = 2; i <= 9; i++) {
                sequencingService.ingest(LocationEvent.builder()
                        .topic(TEST_TOPIC_EVICTION)
                        .tripId("TRIP-EVIC")
                        .cabId("CAB-EVIC")
                        .latitude(12.0 + i)
                        .longitude(77.0 + i)
                        .build());
            }

            // Event 10: Critical SOS Alert
            sequencingService.ingest(SOSEvent.builder()
                    .topic(TEST_TOPIC_EVICTION)
                    .tripId("TRIP-EVIC")
                    .alertType("PANIC_BUTTON")
                    .severity("CRITICAL")
                    .message("Driver unresponsive")
                    .build());

            // Buffer capacity is 4, total events is 10. Buffer contains sequences: 7, 8, 9, 10.
            // Client reconnects asking for seq 3 (which was evicted).
            HydrationResult result = hydrationService.hydrate(TEST_TOPIC_EVICTION, 3L);

            assertThat(result.getType()).isEqualTo(HydrationResult.HydrationType.SNAPSHOT);
            assertThat(result.getSnapshot()).isNotNull();

            TopicSnapshot snapshot = result.getSnapshot();
            assertThat(snapshot.getTopic()).isEqualTo(TEST_TOPIC_EVICTION);
            assertThat(snapshot.getLastSequenceNumber()).isEqualTo(10L);

            // Verify snapshot contains current aggregated state across all event types
            assertThat(snapshot.getLatestStatus()).isNotNull();
            assertThat(snapshot.getLatestStatus().getStatus()).isEqualTo("TRIP_STARTED");

            assertThat(snapshot.getLatestLocation()).isNotNull();
            assertThat(snapshot.getLatestLocation().getLatitude()).isEqualTo(21.0); // 12.0 + 9

            assertThat(snapshot.getActiveSOS()).isNotNull();
            assertThat(snapshot.getActiveSOS().getAlertType()).isEqualTo("PANIC_BUTTON");
            assertThat(snapshot.getActiveSOS().getSeverity()).isEqualTo("CRITICAL");

        } finally {
            sequencingService.setMaxBufferSize(originalCapacity);
        }
    }

    @Test
    @DisplayName("Cross-Node Pub/Sub: broadcasts event across Redis channel and delivers to subscriber")
    void testCrossNodePubSubBroadcast() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Event> receivedEvent = new AtomicReference<>();

        java.util.function.Consumer<Event> testListener = event -> {
            if (TEST_TOPIC_1.equals(event.getTopic()) && event instanceof SOSEvent) {
                receivedEvent.set(event);
                latch.countDown();
            }
        };

        eventSubscriber.addListener(testListener);

        try {
            SOSEvent emergency = SOSEvent.builder()
                    .topic(TEST_TOPIC_1)
                    .tripId("TRIP-999")
                    .cabId("CAB-999")
                    .employeeId("EMP-42")
                    .officeId("BLR-CAMPUS")
                    .alertType("PANIC_BUTTON")
                    .message("Immediate response needed")
                    .severity("CRITICAL")
                    .build();

            sequencingService.ingest(emergency);

            boolean received = latch.await(3, TimeUnit.SECONDS);
            assertThat(received).isTrue();

            Event broadcasted = receivedEvent.get();
            assertThat(broadcasted).isNotNull();
            assertThat(broadcasted).isInstanceOf(SOSEvent.class);
            SOSEvent actualSos = (SOSEvent) broadcasted;
            assertThat(actualSos.getTopic()).isEqualTo(TEST_TOPIC_1);
            assertThat(actualSos.getSequenceNumber()).isEqualTo(1L);
            assertThat(actualSos.getAlertType()).isEqualTo("PANIC_BUTTON");
            assertThat(actualSos.getPriority()).isEqualTo(0);

        } finally {
            eventSubscriber.removeListener(testListener);
        }
    }

    @Test
    @DisplayName("Concurrent Ingestion: preserves strict monotonic ordering under high concurrent load")
    void testConcurrentIngestionOrdering() throws Exception {
        int threadCount = 10;
        int eventsPerThread = 10;
        int totalEvents = threadCount * eventsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        List<Event> ingestedEvents = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    for (int i = 0; i < eventsPerThread; i++) {
                        Event e = sequencingService.ingest(LocationEvent.builder()
                                .topic(TEST_TOPIC_1)
                                .cabId("CAB-" + threadId)
                                .latitude(12.0 + threadId)
                                .longitude(77.0 + i)
                                .build());
                        ingestedEvents.add(e);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(ingestedEvents).hasSize(totalEvents);

        // Sequence numbers must be uniquely 1 to totalEvents
        Set<Long> sequenceNumbers = new HashSet<>();
        for (Event e : ingestedEvents) {
            sequenceNumbers.add(e.getSequenceNumber());
        }
        assertThat(sequenceNumbers).hasSize(totalEvents);
        assertThat(sequenceNumbers).containsExactlyInAnyOrderElementsOf(
                java.util.stream.LongStream.rangeClosed(1, totalEvents).boxed().toList()
        );
    }

    @Test
    @DisplayName("Example Use Case: Employee in lift for 40 seconds receives 3 missed status changes in order, and only the latest location")
    void testLiftFor40SecondsReconnectionHydration() {
        String liftTopic = "trip:TRIP-LIFT-40S";
        cleanTopic(liftTopic);

        // Sequence 1: Initial trip start before entering elevator
        sequencingService.ingest(StatusEvent.builder()
                .topic(liftTopic)
                .tripId("TRIP-LIFT-40S")
                .status("BOOKING_CONFIRMED")
                .build());

        long lastReceivedSeqOnPhone = 1L; // Phone enters elevator here!

        // During 40 seconds in elevator:
        // Cabs stream 10 location updates
        for (int i = 1; i <= 10; i++) {
            sequencingService.ingest(LocationEvent.builder()
                    .topic(liftTopic)
                    .cabId("CAB-LIFT-1")
                    .latitude(12.900 + (i * 0.001))
                    .longitude(77.500 + (i * 0.001))
                    .build());
        }

        // Status change 1: DRIVER_ARRIVED
        sequencingService.ingest(StatusEvent.builder()
                .topic(liftTopic)
                .tripId("TRIP-LIFT-40S")
                .status("DRIVER_ARRIVED")
                .build());

        // Cabs stream another 10 location updates
        for (int i = 11; i <= 20; i++) {
            sequencingService.ingest(LocationEvent.builder()
                    .topic(liftTopic)
                    .cabId("CAB-LIFT-1")
                    .latitude(12.900 + (i * 0.001))
                    .longitude(77.500 + (i * 0.001))
                    .build());
        }

        // Status change 2: PASSENGER_BOARDED
        sequencingService.ingest(StatusEvent.builder()
                .topic(liftTopic)
                .tripId("TRIP-LIFT-40S")
                .status("PASSENGER_BOARDED")
                .build());

        // Cabs stream another 10 location updates
        for (int i = 21; i <= 30; i++) {
            sequencingService.ingest(LocationEvent.builder()
                    .topic(liftTopic)
                    .cabId("CAB-LIFT-1")
                    .latitude(12.900 + (i * 0.001))
                    .longitude(77.500 + (i * 0.001))
                    .build());
        }

        // Status change 3: TRIP_STARTED
        sequencingService.ingest(StatusEvent.builder()
                .topic(liftTopic)
                .tripId("TRIP-LIFT-40S")
                .status("TRIP_STARTED")
                .build());

        // Final 10 location updates
        for (int i = 31; i <= 40; i++) {
            sequencingService.ingest(LocationEvent.builder()
                    .topic(liftTopic)
                    .cabId("CAB-LIFT-1")
                    .latitude(12.900 + (i * 0.001))
                    .longitude(77.500 + (i * 0.001))
                    .build());
        }

        // Phone exits elevator after 40 seconds, reconnects passing last-sequence-number: 1
        HydrationResult result = hydrationService.hydrate(liftTopic, lastReceivedSeqOnPhone, true);

        assertThat(result.getType()).isEqualTo(HydrationResult.HydrationType.REPLAY);
        List<Event> delivered = result.getMissedEvents();

        // Exactly 4 events delivered: 3 status changes + only 1 (the latest) location!
        assertThat(delivered).hasSize(4);

        // Verify the 3 status events were delivered in order
        List<StatusEvent> statuses = delivered.stream()
                .filter(e -> e instanceof StatusEvent)
                .map(e -> (StatusEvent) e)
                .toList();
        assertThat(statuses).hasSize(3);
        assertThat(statuses.get(0).getStatus()).isEqualTo("DRIVER_ARRIVED");
        assertThat(statuses.get(1).getStatus()).isEqualTo("PASSENGER_BOARDED");
        assertThat(statuses.get(2).getStatus()).isEqualTo("TRIP_STARTED");

        // Verify ONLY the latest location was delivered (latitude = 12.940, seq = 44)
        List<LocationEvent> locations = delivered.stream()
                .filter(e -> e instanceof LocationEvent)
                .map(e -> (LocationEvent) e)
                .toList();
        assertThat(locations).hasSize(1);
        assertThat(locations.get(0).getLatitude()).isEqualTo(12.940);
        assertThat(locations.get(0).getSequenceNumber()).isEqualTo(44L);

        cleanTopic(liftTopic);
    }
}

