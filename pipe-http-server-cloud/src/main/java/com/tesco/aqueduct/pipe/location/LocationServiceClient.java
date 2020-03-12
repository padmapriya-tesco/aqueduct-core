package com.tesco.aqueduct.pipe.location;

import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;

@Client("${location.service.cluster.url}")
public interface LocationServiceClient {

    @Get(value = "${location.service.cluster.path}/{locationUuid}", consumes = "application/json")
    LocationServiceClusterResponse getClusters(
        @Header("TraceId") String traceId,
        String locationUuid
    );
}
