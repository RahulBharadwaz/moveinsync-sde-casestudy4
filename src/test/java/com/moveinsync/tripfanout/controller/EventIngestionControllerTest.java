package com.moveinsync.tripfanout.controller;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import com.moveinsync.tripfanout.model.*;
import com.moveinsync.tripfanout.service.EventSequencingService;
import com.moveinsync.tripfanout.service.StateHydrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("null")
class EventIngestionControllerTest {

    @Mock
    private EventSequencingService sequencingService;

    @Mock
    private StateHydrationService hydrationService;

    @Mock
    private GatewayQueueManager gatewayQueueManager;

    @Mock
    private com.moveinsync.tripfanout.service.OnCallMetricsService onCallMetricsService;

    @InjectMocks
    private EventIngestionController controller;

    @Test
    @DisplayName("REST API: Ingest location event successfully")
    void testIngestLocation() {
        LocationEvent loc = LocationEvent.builder()
                .topic("trip:TRIP-101")
                .cabId("CAB-01")
                .latitude(12.9716)
                .longitude(77.5946)
                .speedKmph(45.0)
                .sequenceNumber(1L)
                .build();

        when(sequencingService.ingest(any(LocationEvent.class))).thenReturn(loc);

        ResponseEntity<Event> response = controller.ingestLocation(loc);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getTopic()).isEqualTo("trip:TRIP-101");
        verify(sequencingService).ingest(loc);
    }

    @Test
    @DisplayName("REST API: Ingest status transition event successfully")
    void testIngestStatus() {
        StatusEvent status = StatusEvent.builder()
                .topic("trip:TRIP-101")
                .tripId("TRIP-101")
                .status("DRIVER_ASSIGNED")
                .sequenceNumber(2L)
                .build();

        when(sequencingService.ingest(any(StatusEvent.class))).thenReturn(status);

        ResponseEntity<Event> response = controller.ingestStatus(status);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(((StatusEvent) response.getBody()).getStatus()).isEqualTo("DRIVER_ASSIGNED");
        verify(sequencingService).ingest(status);
    }

    @Test
    @DisplayName("REST API: Ingest SOS emergency alert successfully")
    void testIngestSOS() {
        SOSEvent sos = SOSEvent.builder()
                .topic("trip:TRIP-101")
                .tripId("TRIP-101")
                .alertType("PANIC_BUTTON")
                .severity("CRITICAL")
                .sequenceNumber(3L)
                .build();

        when(sequencingService.ingest(any(SOSEvent.class))).thenReturn(sos);

        ResponseEntity<Event> response = controller.ingestSOS(sos);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(((SOSEvent) response.getBody()).getAlertType()).isEqualTo("PANIC_BUTTON");
        verify(sequencingService).ingest(sos);
    }

    @Test
    @DisplayName("REST API: Retrieve topic snapshot successfully")
    void testGetTopicSnapshot() {
        TopicSnapshot snapshot = TopicSnapshot.builder()
                .topic("trip:TRIP-101")
                .lastSequenceNumber(10L)
                .latestStatus(StatusEvent.builder().status("TRIP_STARTED").build())
                .build();

        when(hydrationService.getSnapshot("trip:TRIP-101")).thenReturn(snapshot);

        ResponseEntity<TopicSnapshot> response = controller.getTopicSnapshot("trip:TRIP-101");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getTopic()).isEqualTo("trip:TRIP-101");
        assertThat(response.getBody().getLastSequenceNumber()).isEqualTo(10L);
    }

    @Test
    @DisplayName("REST API: Retrieve on-call observability metrics")
    void testGetMetrics() {
        Map<String, Object> mockReport = Map.of(
                "subscriptionRejections", 3L,
                "hydrationRetries", 5L,
                "uptimeSeconds", 120L
        );
        when(onCallMetricsService.getOnCallReport()).thenReturn(mockReport);

        ResponseEntity<Map<String, Object>> response = controller.getMetrics();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("subscriptionRejections")).isEqualTo(3L);
        assertThat(response.getBody().get("hydrationRetries")).isEqualTo(5L);
    }
}
