package com.tesco.aqueduct.pipe.location

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.stehno.ersatz.junit.ErsatzServerRule
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class LocationServiceClientIntegrationSpec extends Specification {

    private final static String LOCATION_CLUSTER_PATH = "/v4/clusters/locations"
    private final static String ISSUE_TOKEN_PATH = "v4/issue-token/token"
    private final static String ACCESS_TOKEN = "some_encrypted_token"
    @Shared
    @AutoCleanup
    ErsatzServer locationMockService
    @Shared
    @AutoCleanup
    ErsatzServer identityServiceMock
    @Shared
    @AutoCleanup
    ApplicationContext context
    @Shared
    def CLIENT_ID = "someClientId"
    @Shared
    def CLIENT_SECRET = "someClientSecret"

    def setupSpec() {
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

    def setup() {
        locationMockService.clearExpectations()
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

    private void locationServiceReturningListOfClustersForGiven(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_CLUSTER_PATH + "/" + locationUuid) {
                header("TraceId", "someTraceId")
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
                                "origin": "PRICE"
                            },
                            {
                                "id": "cluster_B",
                                "name": "Cluster B",
                                "origin": "MARKETING"
                            }
                        ],
                        "totalCount": 2
                    }
               """)
                }
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

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
