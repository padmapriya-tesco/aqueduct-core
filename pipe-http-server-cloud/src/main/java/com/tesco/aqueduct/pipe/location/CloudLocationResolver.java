package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.Cluster;
import com.tesco.aqueduct.pipe.api.LocationResolver;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public class CloudLocationResolver implements LocationResolver {

    private final LocationServiceClient locationServiceClient;

    public CloudLocationResolver(@NotNull LocationServiceClient locationServiceClient) {
        if (locationServiceClient == null) {
            throw new IllegalArgumentException("Location service client passed in the constructor cannot be null");
        }
        this.locationServiceClient = locationServiceClient;
    }

    @Override
    public List<Cluster> resolve(@NotNull String locationId) {
        if (locationId == null) {
            throw new IllegalArgumentException("Location id cannot be null");
        }
        return locationServiceClient.getClusters(UUID.randomUUID().toString(), locationId).getClusters();
    }
}
