package com.moveinsync.tripfanout.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Lifecycle status change event (e.g., DRIVER_ASSIGNED, CAB_ARRIVED, TRIP_STARTED, DELAYED, COMPLETED).
 * Strictly ordered and non-conflatable (never dropped).
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StatusEvent extends Event {

    private String tripId;
    private String cabId;
    private String status; // e.g. DRIVER_ASSIGNED, CAB_ARRIVED, TRIP_STARTED, DELAYED, TRIP_COMPLETED, CANCELLED
    private String reason;
    private Integer delayMinutes;

    @Override
    public int getPriority() {
        return 1; // Medium priority lane
    }

    @Override
    public boolean isConflatable() {
        return false; // Status events must never be dropped or conflated
    }
}

