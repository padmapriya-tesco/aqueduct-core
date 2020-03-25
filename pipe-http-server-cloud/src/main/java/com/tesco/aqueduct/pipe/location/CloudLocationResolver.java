package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.Cluster;
import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

public class CloudLocationResolver implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(CloudLocationResolver.class));

    private final LocationServiceClient locationServiceClient;

    public CloudLocationResolver(@NotNull LocationServiceClient locationServiceClient) {
        this.locationServiceClient = locationServiceClient;
    }

    @Override
    public List<Cluster> resolve(@NotNull String locationId) {
        final String traceId = UUID.randomUUID().toString();
        try {
            return locationServiceClient.getClusters(traceId, locationId).getClusters();
        } catch (final HttpClientResponseException exception) {
            LOG.error("resolve", "trace_id: " + traceId, exception);
            if (exception.getStatus().getCode() > 499) {
                throw new LocationServiceUnavailableException("Unexpected error from location service with status - " + exception.getStatus());
            } else {
                throw exception;
            }
        }
    }
}
