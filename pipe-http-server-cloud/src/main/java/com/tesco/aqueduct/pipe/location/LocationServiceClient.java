package com.tesco.aqueduct.pipe.location;

import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;

@Client("${location.url}")
public interface LocationServiceClient {
    @Get(value = "${location.get.cluster.path}/{locationUuid}", consumes = "application/json")
    @Cacheable(value = "cluster-cache", parameters = "locationUuid")
    LocationServiceClusterResponse getClusters(
        @Header("TraceId") String traceId,
        String locationUuid
    );
}
