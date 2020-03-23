package com.tesco.aqueduct.pipe.location;

import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;

@Client(id="location")
public interface LocationServiceClient {

    @Get("${location.service.clusters.path}/{?locationUuid}")
    Flowable<LocationServiceClusterResponse> getClusters(
        @Header("TraceId") String traceId,
        String locationUuid
    );
}
