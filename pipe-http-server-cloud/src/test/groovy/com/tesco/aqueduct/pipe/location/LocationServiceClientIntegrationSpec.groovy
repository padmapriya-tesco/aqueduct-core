package com.tesco.aqueduct.pipe.location

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Flowable
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class LocationServiceClientIntegrationSpec extends Specification {

    private final static String LOCATION_CLUSTER_PATH = "/v4/clusters/locations"
    @Shared @AutoCleanup ErsatzServer locationMockService
    @Shared @AutoCleanup ApplicationContext context

    def setupSpec() {
        locationMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService.start()

        context = ApplicationContext
                .build()
                .mainClass(EmbeddedServer)
                .properties(
                        parseYamlConfig(
                        """
                            location:
                                service: 
                                    clusters:
                                        path:   "${LOCATION_CLUSTER_PATH}"
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

    def "A list of clusters are provided for given location Uuid"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "location service returning list of clusters for a given Uuid"
        locationMockService.expectations {
            get(LOCATION_CLUSTER_PATH + "/" + locationUuid) {
               header("Content-Type", "application/json")
               called(1)

               responder {
                   header("Content-Type", "application/json")
                   header("TraceId", "someTraceId")
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

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        def clusters = locationServiceClient.getClusters("someTraceId", locationUuid)

        then:
        clusters == ["cluster_A", "cluster_B"]
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
