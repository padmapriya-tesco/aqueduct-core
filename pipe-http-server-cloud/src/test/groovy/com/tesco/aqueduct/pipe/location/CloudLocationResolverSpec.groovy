package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.Cluster
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.convert.value.MutableConvertibleValues
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.simple.SimpleHttpHeaders
import spock.lang.Specification

class CloudLocationResolverSpec extends Specification {

    def "Location service is invoked to fetch cluster for given location Uuid"() {
        given:
        def locationUuid = "someLocationUuid"

        and:
        def locationServiceClient = Mock(LocationServiceClient)

        when:
        def locations = new CloudLocationResolver(locationServiceClient).resolve(locationUuid)

        then:
        1 * locationServiceClient.getClusters(_, locationUuid) >>
            new LocationServiceClusterResponse([new Cluster("cluster_A"), new Cluster("cluster_B")])

        and: "target clusters are returned"
        locations == [new Cluster("cluster_A"), new Cluster("cluster_B")]
    }

    def "Location service throws LocationServiceUnavailable error when client throws http client response error with status code 5xx"() {
        given:
        def locationUuid = "someLocationUuid"

        and:
        def locationServiceClient = Mock(LocationServiceClient)

        when:
        new CloudLocationResolver(locationServiceClient).resolve(locationUuid)

        then:
        1 * locationServiceClient.getClusters(_, locationUuid) >>
            {
                throw new HttpClientResponseException(
                    "some error message from location service",
                    httpResponseWithStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                )
            }

        then: "LocationServiceUnavailableException is thrown"
        thrown(LocationServiceUnavailableException)
    }

    def "Location service propagates client error when client throws http client response error with status code 4xx"() {
        given:
        def locationUuid = "someLocationUuid"

        and:
        def locationServiceClient = Mock(LocationServiceClient)

        when:
        new CloudLocationResolver(locationServiceClient).resolve(locationUuid)

        then:
        1 * locationServiceClient.getClusters(_, locationUuid) >>
            {
                throw new HttpClientResponseException(
                    "some error message from location service",
                    httpResponseWithStatus(HttpStatus.BAD_REQUEST)
                )
            }

        then: "Http client error is propagated"
        thrown(HttpClientResponseException)
    }

    private HttpResponse<Object> httpResponseWithStatus(HttpStatus httpStatus) {
        new HttpResponse<Object>() {
            @Override
            HttpStatus getStatus() {
                return httpStatus
            }

            @Override
            HttpHeaders getHeaders() {
                new SimpleHttpHeaders(ConversionService.SHARED)
            }

            @Override
            MutableConvertibleValues<Object> getAttributes() {
                return null
            }

            @Override
            Optional<Object> getBody() {
                return Optional.empty()
            }
        }
    }
}
