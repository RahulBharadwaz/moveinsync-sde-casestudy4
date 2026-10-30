package com.moveinsync.tripfanout.security;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Directory for trip context metadata (tenant, employee, office, cab).
 * Used during subscription authorization to enforce tenant isolation and role restrictions.
 */
@Component
public class TripMetadataRegistry {

    private final Map<String, TripMetadata> trips = new ConcurrentHashMap<>();
    private final Map<String, String> officeToTenantMap = new ConcurrentHashMap<>();
    private final Map<String, String> cabToTenantMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void initDefaults() {
        // Tenant A trips
        registerTrip(TripMetadata.builder()
                .tripId("TRIP-101")
                .tenantId("TENANT-A")
                .employeeId("emp-1")
                .officeId("OFFICE-BLR")
                .cabId("CAB-01")
                .build());

        registerTrip(TripMetadata.builder()
                .tripId("TRIP-102")
                .tenantId("TENANT-A")
                .employeeId("emp-2")
                .officeId("OFFICE-BLR")
                .cabId("CAB-02")
                .build());

        registerTrip(TripMetadata.builder()
                .tripId("TRIP-103")
                .tenantId("TENANT-A")
                .employeeId("emp-3")
                .officeId("OFFICE-HYD")
                .cabId("CAB-03")
                .build());

        // Tenant B trips (for cross-tenant security testing)
        registerTrip(TripMetadata.builder()
                .tripId("TRIP-201")
                .tenantId("TENANT-B")
                .employeeId("emp-other-tenant")
                .officeId("OFFICE-NYC")
                .cabId("CAB-99")
                .build());

        // Offices mapping
        registerOffice("OFFICE-BLR", "TENANT-A");
        registerOffice("OFFICE-HYD", "TENANT-A");
        registerOffice("OFFICE-NYC", "TENANT-B");

        // Cabs mapping
        registerCab("CAB-01", "TENANT-A");
        registerCab("CAB-02", "TENANT-A");
        registerCab("CAB-03", "TENANT-A");
        registerCab("CAB-99", "TENANT-B");
    }

    public void registerTrip(TripMetadata metadata) {
        if (metadata != null && metadata.getTripId() != null) {
            trips.put(metadata.getTripId(), metadata);
            if (metadata.getOfficeId() != null && metadata.getTenantId() != null) {
                officeToTenantMap.put(metadata.getOfficeId(), metadata.getTenantId());
            }
            if (metadata.getCabId() != null && metadata.getTenantId() != null) {
                cabToTenantMap.put(metadata.getCabId(), metadata.getTenantId());
            }
        }
    }

    public void registerOffice(String officeId, String tenantId) {
        officeToTenantMap.put(officeId, tenantId);
    }

    public void registerCab(String cabId, String tenantId) {
        cabToTenantMap.put(cabId, tenantId);
    }

    public Optional<TripMetadata> findTrip(String tripId) {
        return Optional.ofNullable(trips.get(tripId));
    }

    public Optional<String> getTenantForOffice(String officeId) {
        return Optional.ofNullable(officeToTenantMap.get(officeId));
    }

    public Optional<String> getTenantForCab(String cabId) {
        return Optional.ofNullable(cabToTenantMap.get(cabId));
    }
}

