package com.moveinsync.tripfanout.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Emergency SOS alert event.
 * Highest priority lane (priority = 0). Must reach security desks within 2 seconds.
 * Strictly non-conflatable (never dropped).
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SOSEvent extends Event {

    private String tripId;
    private String cabId;
    private String employeeId;
    private String officeId;
    private double latitude;
    private double longitude;
    private String alertType; // e.g. PANIC_BUTTON, ROUTE_DEVIATION, MEDICAL_EMERGENCY
    private String message;
    private String severity; // e.g. CRITICAL, HIGH

    @Override
    public int getPriority() {
        return 0; // Highest priority - skips ahead of all other traffic
    }

    @Override
    public boolean isConflatable() {
        return false; // SOS events must never be dropped or conflated
    }
}

