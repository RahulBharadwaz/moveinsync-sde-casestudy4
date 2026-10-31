package com.moveinsync.tripfanout.controller;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import com.moveinsync.tripfanout.model.*;
import com.moveinsync.tripfanout.service.EventSequencingService;
import com.moveinsync.tripfanout.service.StateHydrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST ingestion controller for publishing telematics, trip status changes, and emergency SOS alerts.
 * Bridges HTTP client/driver ingestion requests into the EventSequencingService and Redis pub/sub cluster.
 * Also exposes topic snapshots and node metrics for observability.
 */
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class EventIngestionController {

    private final EventSequencingService sequencingService;
    private final StateHydrationService hydrationService;
    private final GatewayQueueManager gatewayQueueManager;
    private final com.moveinsync.tripfanout.service.OnCallMetricsService onCallMetricsService;

    @PostMapping("/events/location")
    public ResponseEntity<Event> ingestLocation(@RequestBody LocationEvent locationEvent) {
        log.info("Ingesting location update for cab [{}] on topic [{}]",
                locationEvent.getCabId(), locationEvent.getTopic());
        Event ingested = sequencingService.ingest(locationEvent);
        return ResponseEntity.ok(ingested);
    }

    @PostMapping("/events/status")
    public ResponseEntity<Event> ingestStatus(@RequestBody StatusEvent statusEvent) {
        log.info("Ingesting status transition [{}] for trip [{}] on topic [{}]",
                statusEvent.getStatus(), statusEvent.getTripId(), statusEvent.getTopic());
        Event ingested = sequencingService.ingest(statusEvent);
        return ResponseEntity.ok(ingested);
    }

    @PostMapping("/events/sos")
    public ResponseEntity<Event> ingestSOS(@RequestBody SOSEvent sosEvent) {
        log.warn("EMERGENCY: Ingesting SOS alert [{}] for trip [{}] on topic [{}]",
                sosEvent.getAlertType(), sosEvent.getTripId(), sosEvent.getTopic());
        Event ingested = sequencingService.ingest(sosEvent);
        return ResponseEntity.ok(ingested);
    }

    @PostMapping("/events")
    public ResponseEntity<Event> ingestGeneric(@RequestBody Event event) {
        log.info("Ingesting generic event [{}] on topic [{}]", event.getEventType(), event.getTopic());
        Event ingested = sequencingService.ingest(event);
        return ResponseEntity.ok(ingested);
    }

    @GetMapping("/topics/{topic}/snapshot")
    public ResponseEntity<TopicSnapshot> getTopicSnapshot(@PathVariable String topic) {
        TopicSnapshot snapshot = hydrationService.getSnapshot(topic);
        return ResponseEntity.ok(snapshot);
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        return ResponseEntity.ok(onCallMetricsService.getOnCallReport());
    }
}

