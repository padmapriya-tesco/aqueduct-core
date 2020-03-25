package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.Cluster;
import com.tesco.aqueduct.pipe.api.LocationResolver;
import io.micronaut.http.client.exceptions.HttpClientResponseException;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public class CloudLocationResolver implements LocationResolver {

    private final LocationServiceClient locationServiceClient;

    public CloudLocationResolver(@NotNull LocationServiceClient locationServiceClient) {
        this.locationServiceClient = locationServiceClient;
    }

    @Override
    public List<Cluster> resolve(@NotNull String locationId) {
        try {
            return locationServiceClient.getClusters(UUID.randomUUID().toString(), locationId).getClusters();
        } catch(final HttpClientResponseException exception) {
            if (exception.getStatus().getCode() > 499) {
                throw new LocationServiceUnavailableException("Unexpected error from location service");
            } else {
                throw exception;
            }
        }
    }
}
