package com.moveinsync.tripfanout.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

/**
 * Base abstract Event for trip updates.
 * Supports polymorphic JSON serialization/deserialization and priority-based ordering.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "eventType",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LocationEvent.class, name = "LOCATION"),
        @JsonSubTypes.Type(value = StatusEvent.class, name = "STATUS"),
        @JsonSubTypes.Type(value = SOSEvent.class, name = "SOS")
})
public abstract class Event implements Serializable, Comparable<Event> {

    private String eventId;

    /**
     * Topic identifying the target stream (cab/trip/office ID, e.g. "cab:CAB-101", "trip:TRIP-500", "office:BLR-OFFICE-1")
     */
    private String topic;

    /**
     * Monotonically increasing per-topic sequence number assigned by the ordering authority.
     */
    private long sequenceNumber;

    /**
     * Epoch timestamp in milliseconds.
     */
    private long timestamp;

    /**
     * Type of event (LOCATION, STATUS, SOS).
     */
    private EventType eventType;

    /**
     * Numeric priority for lane processing.
     * Lower value indicates higher priority (SOS = 0, STATUS = 1, LOCATION = 2).
     */
    @JsonIgnore
    public abstract int getPriority();

    /**
     * Indicates whether this event can be conflated (only keeping the latest update per entity)
     * when a subscriber experiences back-pressure.
     */
    @JsonIgnore
    public abstract boolean isConflatable();

    @Override
    public int compareTo(Event other) {
        if (other == null) {
            return -1;
        }
        // 1. Priority lane: higher priority (lower value) comes first
        int priorityComparison = Integer.compare(this.getPriority(), other.getPriority());
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        // 2. Order by sequence number within the same priority level
        int seqComparison = Long.compare(this.sequenceNumber, other.sequenceNumber);
        if (seqComparison != 0) {
            return seqComparison;
        }
        // 3. Fallback to timestamp
        return Long.compare(this.timestamp, other.timestamp);
    }
}
