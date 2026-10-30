package com.moveinsync.tripfanout.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventModelTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Should correctly serialize and deserialize polymorphic Event subclasses")
    void testPolymorphicSerialization() throws Exception {
        LocationEvent locationEvent = LocationEvent.builder()
                .eventId("loc-1")
                .topic("cab:CAB-101")
                .sequenceNumber(1001L)
                .timestamp(System.currentTimeMillis())
                .eventType(EventType.LOCATION)
                .cabId("CAB-101")
                .tripId("TRIP-500")
                .latitude(12.9716)
                .longitude(77.5946)
                .speedKmph(45.5)
                .bearing(180.0)
                .build();

        String json = objectMapper.writeValueAsString(locationEvent);
        Event deserialized = objectMapper.readValue(json, Event.class);

        assertThat(deserialized).isInstanceOf(LocationEvent.class);
        LocationEvent actual = (LocationEvent) deserialized;
        assertThat(actual.getTopic()).isEqualTo("cab:CAB-101");
        assertThat(actual.getSequenceNumber()).isEqualTo(1001L);
        assertThat(actual.getLatitude()).isEqualTo(12.9716);
        assertThat(actual.isConflatable()).isTrue();
        assertThat(actual.getPriority()).isEqualTo(2);

        SOSEvent sosEvent = SOSEvent.builder()
                .eventId("sos-1")
                .topic("trip:TRIP-500")
                .sequenceNumber(1002L)
                .timestamp(System.currentTimeMillis())
                .eventType(EventType.SOS)
                .tripId("TRIP-500")
                .cabId("CAB-101")
                .employeeId("EMP-99")
                .officeId("BLR-OFFICE-1")
                .alertType("PANIC_BUTTON")
                .message("Immediate assistance requested")
                .severity("CRITICAL")
                .build();

        String sosJson = objectMapper.writeValueAsString(sosEvent);
        Event deserializedSos = objectMapper.readValue(sosJson, Event.class);

        assertThat(deserializedSos).isInstanceOf(SOSEvent.class);
        SOSEvent actualSos = (SOSEvent) deserializedSos;
        assertThat(actualSos.getPriority()).isEqualTo(0);
        assertThat(actualSos.isConflatable()).isFalse();
    }

    @Test
    @DisplayName("Should prioritize SOS events over status and location events in priority order")
    void testPriorityOrdering() {
        LocationEvent location = LocationEvent.builder()
                .sequenceNumber(1L)
                .topic("trip:1")
                .timestamp(100L)
                .build();

        StatusEvent status = StatusEvent.builder()
                .sequenceNumber(2L)
                .topic("trip:1")
                .timestamp(200L)
                .status("TRIP_STARTED")
                .build();

        SOSEvent sos = SOSEvent.builder()
                .sequenceNumber(3L)
                .topic("trip:1")
                .timestamp(300L)
                .alertType("PANIC_BUTTON")
                .build();

        List<Event> events = new ArrayList<>(List.of(location, status, sos));
        Collections.sort(events);

        // SOS (priority 0) first, then Status (priority 1), then Location (priority 2)
        assertThat(events.get(0)).isInstanceOf(SOSEvent.class);
        assertThat(events.get(1)).isInstanceOf(StatusEvent.class);
        assertThat(events.get(2)).isInstanceOf(LocationEvent.class);
    }
}

