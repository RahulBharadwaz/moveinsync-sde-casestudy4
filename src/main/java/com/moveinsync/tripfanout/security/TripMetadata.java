package com.moveinsync.tripfanout.security;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TripMetadata {
    private String tripId;
    private String tenantId;
    private String employeeId;
    private String officeId;
    private String cabId;
}

