package com.moveinsync.tripfanout.engine;

import com.moveinsync.tripfanout.model.Event;
import com.moveinsync.tripfanout.model.LocationEvent;
import com.moveinsync.tripfanout.model.SOSEvent;
import com.moveinsync.tripfanout.model.StatusEvent;
import org.springframework.lang.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Isolated, bounded, priority-aware, and conflating outbound queue for a single WebSocket connection.
 *
 * Guarantees:
 * 1. Bounded Back-Pressure: A slow client never blocks the gateway or starves other connections.
 * 2. Conflation: For location updates, keeps only the latest location per cab when the client falls behind.
 * 3. Inviolable Delivery: Status changes and SOS events are NEVER dropped.
 * 4. Priority Lane: SOS events skip ahead of all status and location traffic in the delivery order.
 */
@Slf4j
public class ClientSessionQueue {

    @Getter
    private final String sessionId;

    @Getter
    private final int maxCapacity;

    @Getter
    private final boolean conflationEnabled;

    // Priority Lane 0 (Highest priority - Emergency SOS alerts)
    private final Queue<SOSEvent> sosLane = new ConcurrentLinkedQueue<>();

    // Priority Lane 1 (Medium priority - Strictly ordered trip lifecycle status events)
    private final Queue<StatusEvent> statusLane = new ConcurrentLinkedQueue<>();

    // Priority Lane 2 (Normal priority - Conflated location updates keyed by cabId)
    private final Map<String, LocationEvent> locationByCab = new ConcurrentHashMap<>();
    private final Queue<String> cabOrderQueue = new LinkedBlockingQueue<>();

    // Metrics counters
    @Getter
    private final AtomicLong conflatedCount = new AtomicLong(0);

    @Getter
    private final AtomicLong droppedLocationCount = new AtomicLong(0);

    @Getter
    private final AtomicLong totalEnqueuedCount = new AtomicLong(0);

    @Getter
    private final AtomicLong totalPolledCount = new AtomicLong(0);

    public ClientSessionQueue(String sessionId, int maxCapacity, boolean conflationEnabled) {
        this.sessionId = sessionId;
        this.maxCapacity = Math.max(5, maxCapacity);
        this.conflationEnabled = conflationEnabled;
    }

    public ClientSessionQueue(String sessionId, int maxCapacity) {
        this(sessionId, maxCapacity, true);
    }

    /**
     * Non-blocking enqueue of an outbound event.
     * Always completes in O(1) without blocking the calling gateway or ingestion thread.
     *
     * @param event The event to enqueue
     * @return true if accepted/conflated
     */
    public boolean enqueue(@Nullable Event event) {
        if (event == null) {
            return false;
        }

        totalEnqueuedCount.incrementAndGet();

        // 1. Priority Lane 0: SOS alerts skip ahead of all traffic and are NEVER dropped
        if (event instanceof SOSEvent sos) {
            sosLane.offer(sos);
            log.trace("SOS alert [{}] queued in Priority Lane for session [{}]", sos.getEventId(), sessionId);
            return true;
        }

        // 2. Priority Lane 1: Status events preserve strict ordering and are NEVER dropped
        if (event instanceof StatusEvent status) {
            statusLane.offer(status);
            log.trace("Status event [{}] queued for session [{}]", status.getEventId(), sessionId);
            return true;
        }

        // 3. Priority Lane 2: Location telemetry with cab-based conflation under back-pressure
        if (event instanceof LocationEvent loc) {
            enqueueLocation(loc);
            return true;
        }

        return true;
    }

    private void enqueueLocation(LocationEvent loc) {
        String cabKey = resolveCabKey(loc);

        if (conflationEnabled) {
            LocationEvent previous = locationByCab.put(cabKey, loc);
            if (previous != null) {
                // Existing pending location for this cab was conflated with newer telemetry!
                conflatedCount.incrementAndGet();
                log.trace("Conflated location for cab [{}] on session [{}]: seq {} -> {}",
                        cabKey, sessionId, previous.getSequenceNumber(), loc.getSequenceNumber());
                return;
            }
        }

        // New cab entry: check if queue capacity is reached
        if (size() >= maxCapacity) {
            // Evict oldest pending location update to protect heap memory
            String oldestCab = cabOrderQueue.poll();
            if (oldestCab != null) {
                locationByCab.remove(oldestCab);
                droppedLocationCount.incrementAndGet();
                log.trace("Queue bounded at capacity [{}]. Evicted oldest location for cab [{}] on session [{}]",
                        maxCapacity, oldestCab, sessionId);
            }
        }

        cabOrderQueue.offer(cabKey);
        locationByCab.put(cabKey, loc);
    }

    private String resolveCabKey(LocationEvent loc) {
        if (loc.getCabId() != null && !loc.getCabId().isBlank()) {
            return loc.getCabId();
        }
        if (loc.getTopic() != null && !loc.getTopic().isBlank()) {
            return loc.getTopic();
        }
        return "CAB-DEFAULT";
    }

    /**
     * Polls the next event to transmit over the WebSocket connection.
     * Strictly adheres to priority lane ordering:
     * SOS (0) -> Status (1) -> Location (2).
     *
     * @return Next prioritized event, or null if queue is empty
     */
    @Nullable
    public Event poll() {
        // Priority Lane 1: SOS alerts always take precedence
        SOSEvent sos = sosLane.poll();
        if (sos != null) {
            totalPolledCount.incrementAndGet();
            return sos;
        }

        // Priority Lane 2: Lifecycle status changes (FIFO sequence order, never dropped)
        StatusEvent status = statusLane.poll();
        if (status != null) {
            totalPolledCount.incrementAndGet();
            return status;
        }

        // Priority Lane 3: Latest conflated location per cab
        while (!cabOrderQueue.isEmpty()) {
            String cabKey = cabOrderQueue.poll();
            if (cabKey != null) {
                LocationEvent loc = locationByCab.remove(cabKey);
                if (loc != null) {
                    totalPolledCount.incrementAndGet();
                    return loc;
                }
            }
        }

        return null;
    }

    /**
     * Returns total pending messages across all lanes.
     */
    public int size() {
        return sosLane.size() + statusLane.size() + locationByCab.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int getSosLaneSize() {
        return sosLane.size();
    }

    public int getStatusLaneSize() {
        return statusLane.size();
    }

    public int getLocationLaneSize() {
        return locationByCab.size();
    }

    public boolean isBackPressured() {
        return size() >= (int) (maxCapacity * 0.8);
    }

    public void clear() {
        sosLane.clear();
        statusLane.clear();
        locationByCab.clear();
        cabOrderQueue.clear();
    }
}

