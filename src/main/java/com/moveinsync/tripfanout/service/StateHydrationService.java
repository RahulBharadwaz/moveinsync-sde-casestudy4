package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.model.*;
import com.moveinsync.tripfanout.repository.TopicStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for hydrating reconnecting subscribers.
 * Replays missed events in order if within the bounded buffer; otherwise hydrates with full current state snapshot.
 * Coordinates with TopicStateRepository.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StateHydrationService {

    private final TopicStateRepository repository;

    /**
     * Hydrate a client resuming subscription on a topic.
     * Raw replay mode: returns all missed events without conflation.
     */
    public HydrationResult hydrate(String topic, long lastSequenceNumber) {
        return hydrate(topic, lastSequenceNumber, false);
    }

    /**
     * Hydrate a client resuming subscription on a topic.
     *
     * @param topic Topic identifier
     * @param lastSequenceNumber Last sequence number successfully processed by the client
     * @param conflateLocations When true (e.g. mobile client resuming after elevator/tunnel),
     *                          conflates intermediate location updates to only return the latest
     *                          location per vehicle while preserving all status & SOS events in order.
     * @return HydrationResult containing either REPLAY list or full SNAPSHOT
     */
    public HydrationResult hydrate(String topic, long lastSequenceNumber, boolean conflateLocations) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }

        long currentSeq = repository.getCurrentSequence(topic);

        // 1. Topic has no events yet
        if (currentSeq == 0) {
            return HydrationResult.replay(topic, lastSequenceNumber, 0, Collections.emptyList());
        }

        // 2. Client is already fully caught up
        if (lastSequenceNumber >= currentSeq) {
            return HydrationResult.replay(topic, lastSequenceNumber, currentSeq, Collections.emptyList());
        }

        // 3. Inspect bounded buffer
        Long minSeq = repository.getBufferMinSequence(topic);
        if (minSeq == null) {
            log.info("Buffer empty for topic {}, falling back to full snapshot hydration", topic);
            return HydrationResult.snapshot(topic, lastSequenceNumber, currentSeq, getSnapshot(topic));
        }

        // 4. Check if the next event the client needs (lastSequenceNumber + 1) has been evicted
        if (lastSequenceNumber < minSeq - 1) {
            log.info("Client requested seq {} on topic {}, but buffer min seq is {}. Evicted! Hydrating snapshot.",
                    lastSequenceNumber, topic, minSeq);
            TopicSnapshot snapshot = getSnapshot(topic);
            return HydrationResult.snapshot(topic, lastSequenceNumber, currentSeq, snapshot);
        }

        // 5. In-buffer replay: Fetch all missed events in score (sequence) order
        List<Event> missedEvents = repository.getBufferRange(
                topic,
                lastSequenceNumber + 1,
                Long.MAX_VALUE
        );

        // Apply in-place location conflation if requested (e.g. 40-second lift use case)
        if (conflateLocations && !missedEvents.isEmpty()) {
            missedEvents = conflateReplayLocations(missedEvents);
        }

        log.debug("Replaying {} missed events for topic {} from seq {} to current {} (conflated: {})",
                missedEvents.size(), topic, lastSequenceNumber, currentSeq, conflateLocations);

        return HydrationResult.replay(topic, lastSequenceNumber, currentSeq, missedEvents);
    }

    /**
     * Conflates intermediate location events during replay while strictly preserving
     * status transitions and SOS emergency alerts in sequence order.
     */
    private List<Event> conflateReplayLocations(List<Event> events) {
        Map<String, LocationEvent> latestLocByCab = new LinkedHashMap<>();
        for (Event e : events) {
            if (e instanceof LocationEvent loc) {
                String cabKey = (loc.getCabId() != null && !loc.getCabId().isBlank())
                        ? loc.getCabId() : "DEFAULT";
                latestLocByCab.put(cabKey, loc);
            }
        }

        Set<String> processedCabs = new HashSet<>();
        List<Event> conflated = new ArrayList<>(events.size());

        for (Event e : events) {
            if (e instanceof StatusEvent || e instanceof SOSEvent) {
                conflated.add(e);
            } else if (e instanceof LocationEvent loc) {
                String cabKey = (loc.getCabId() != null && !loc.getCabId().isBlank())
                        ? loc.getCabId() : "DEFAULT";
                LocationEvent latest = latestLocByCab.get(cabKey);
                if (latest != null && latest.getSequenceNumber() == loc.getSequenceNumber()
                        && !processedCabs.contains(cabKey)) {
                    conflated.add(latest);
                    processedCabs.add(cabKey);
                }
            }
        }
        return conflated;
    }

    /**
     * Retrieve the current aggregated state snapshot for a topic.
     */
    public TopicSnapshot getSnapshot(String topic) {
        return repository.getSnapshot(topic)
                .orElseGet(() -> TopicSnapshot.builder()
                        .topic(topic)
                        .lastSequenceNumber(repository.getCurrentSequence(topic))
                        .build());
    }
}
