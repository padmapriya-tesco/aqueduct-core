package com.tesco.aqueduct.pipe.location

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.stehno.ersatz.junit.ErsatzServerRule
import com.tesco.aqueduct.pipe.api.Cluster
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.client.exceptions.HttpClientException
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class LocationServiceClientIntegrationSpec extends Specification {

    private final static String LOCATION_CLUSTER_PATH = "/v4/clusters/locations"
    private final static String ISSUE_TOKEN_PATH = "v4/issue-token/token"
    private final static String ACCESS_TOKEN = "some_encrypted_token"
    private final static String CLIENT_ID = "someClientId"
    private final static String CLIENT_SECRET = "someClientSecret"
    private final static String CACHE_EXPIRY_HOURS = "1h"

    @Shared
    @AutoCleanup
    ErsatzServer locationMockService
    @Shared
    @AutoCleanup
    ErsatzServer identityServiceMock
    @Shared
    @AutoCleanup
    ApplicationContext context

    def setup() {
        locationMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        identityServiceMock = new ErsatzServerRule({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService.start()
        identityServiceMock.start()

        context = ApplicationContext
                .build()
                .mainClass(EmbeddedServer)
                .properties(
                    parseYamlConfig(
                    """
                    micronaut.caches.cluster-cache..expire-after-write: $CACHE_EXPIRY_HOURS
                    location:
                        service: 
                            cluster:
                                url:    "${locationMockService.getHttpUrl()}"
                                path:   "${LOCATION_CLUSTER_PATH}"
                    authentication:
                        identity:
                            url:                ${identityServiceMock.getHttpUrl()}
                            issue.token.path:   "$ISSUE_TOKEN_PATH"
                            attempts:           3
                            delay:              500ms
                            client:
                                id:         "$CLIENT_ID"
                                secret:     "$CLIENT_SECRET"
                    """
                    )
                )
                .build()

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()
    }

    def "A list of clusters are provided for given location Uuid by authorized location service"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        def clusterResponse = locationServiceClient.getClusters("someTraceId", locationUuid)

        then:
        clusterResponse.clusters == [new Cluster("cluster_A"), new Cluster("cluster_B")]

        and: "location service is called once"
        locationMockService.verify()
    }

    def "location is cached"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        locationServiceClient.getClusters("someTraceId", locationUuid)

        then: "location service is called"
        locationMockService.verify()

        when: "location service is called with the same locationUuid"
        locationServiceClient.getClusters("anotherTraceId", locationUuid)

        then: "location is cached"
        locationMockService.verify()
    }

    def "Unauthorised exception is thrown if token is invalid or missing"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service that fails to issue a token"
        identityIssueTokenFailure()

        and: "a mock for location service is not called"
        locationServiceNotInvoked(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        locationServiceClient.getClusters("someTraceId", locationUuid)

        then: "location service is not called"
        locationMockService.verify()

        and: "an exception is thrown"
        thrown(HttpClientException)
    }

    private void locationServiceReturningListOfClustersForGiven(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_CLUSTER_PATH + "/" + locationUuid) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [
                            {
                                "id": "cluster_A",
                                "name": "Cluster A",
                                "origin": "ORIGIN1"
                            },
                            {
                                "id": "cluster_B",
                                "name": "Cluster B",
                                "origin": "ORIGIN2"
                            }
                        ],
                        "totalCount": 2
                    }
               """)
                }
            }
        }
    }

    private void locationServiceNotInvoked(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_CLUSTER_PATH + "/" + locationUuid) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(0)
            }
        }
    }

    private void identityIssueTokenService() {
        def requestJson = JsonOutput.toJson([
                client_id       : CLIENT_ID,
                client_secret   : CLIENT_SECRET,
                grant_type      : "client_credentials",
                scope           : "internal public",
                confidence_level: 12
        ])

        identityServiceMock.expectations {
            post(ISSUE_TOKEN_PATH) {
                body(requestJson, "application/json")
                header("Accept", "application/vnd.tesco.identity.tokenresponse+json")
                called(1)
                responder {
                    header("Content-Type", "application/vnd.tesco.identity.tokenresponse+json")
                    code(200)
                    body("""
                        {
                            "access_token": "${ACCESS_TOKEN}",
                            "token_type"  : "bearer",
                            "expires_in"  : 1000,
                            "scope"       : "some: scope: value"
                        }
                    """)
                }
            }
        }
    }

    private void identityIssueTokenFailure() {
        def requestJson = JsonOutput.toJson([
            client_id       : CLIENT_ID,
            client_secret   : CLIENT_SECRET,
            grant_type      : "client_credentials",
            scope           : "internal public",
            confidence_level: 12
        ])

        identityServiceMock.expectations {
            post(ISSUE_TOKEN_PATH) {
                body(requestJson, "application/json")
                header("Accept", "application/vnd.tesco.identity.tokenresponse+json")
                called(1)
                responder {
                    code(403)
                }
            }
        }
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
