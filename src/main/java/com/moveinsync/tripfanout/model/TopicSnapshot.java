package com.moveinsync.tripfanout.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Represents the aggregated state snapshot for a given topic (trip/cab/office).
 * Used for fast hydration when reconnecting clients have missed events that were evicted from the buffer.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicSnapshot implements Serializable {

    private String topic;
    private long lastSequenceNumber;
    private long lastTimestamp;
    private LocationEvent latestLocation;
    private StatusEvent latestStatus;
    private SOSEvent activeSOS;

    @JsonIgnore
    public boolean isEmpty() {
        return lastSequenceNumber == 0 && latestLocation == null && latestStatus == null && activeSOS == null;
    }

    /**
     * Applies a new event to the snapshot, advancing sequence number and updating the latest state.
     */
    public void applyEvent(Event event) {
        if (event == null) {
            return;
        }
        this.topic = event.getTopic();
        this.lastSequenceNumber = Math.max(this.lastSequenceNumber, event.getSequenceNumber());
        this.lastTimestamp = Math.max(this.lastTimestamp, event.getTimestamp());

        if (event instanceof LocationEvent loc) {
            this.latestLocation = loc;
        } else if (event instanceof StatusEvent status) {
            this.latestStatus = status;
        } else if (event instanceof SOSEvent sos) {
            if ("RESOLVED".equalsIgnoreCase(sos.getAlertType())) {
                this.activeSOS = null;
            } else {
                this.activeSOS = sos;
            }
        }
    }
}

