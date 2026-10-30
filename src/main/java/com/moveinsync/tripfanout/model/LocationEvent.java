package com.moveinsync.tripfanout.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * High-frequency location telemetry event emitted by cabs.
 * Eligible for conflation under back-pressure (keeping only the latest location per cab).
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class LocationEvent extends Event {

    private String cabId;
    private String tripId;
    private double latitude;
    private double longitude;
    private double speedKmph;
    private double bearing;

    @Override
    public int getPriority() {
        return 2; // Normal priority lane
    }

    @Override
    public boolean isConflatable() {
        return true; // Conflated when client falls behind
    }
}

