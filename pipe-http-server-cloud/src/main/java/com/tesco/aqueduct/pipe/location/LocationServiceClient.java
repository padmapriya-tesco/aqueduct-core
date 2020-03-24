package com.tesco.aqueduct.pipe.location;

import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;

@Client("${location.url}")
public interface LocationServiceClient {
    @Get("/v4/clusters/locations/{locationUuid}")
    @Consumes
    @Cacheable(value = "cluster-cache", parameters = "locationUuid")
    LocationServiceClusterResponse getClusters(
            @Header("TraceId") String traceId,
            String locationUuid
            );
}
