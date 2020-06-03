package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.JsonHelper
import spock.lang.Specification

class LocationServiceClusterResponseSpec extends Specification {

    def "Json deserialization is successful"() {
        given: "a location service cluster response json"
        def locationServiceClusterResponseJson =
            """
                {
                    "clusters": [
                        "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
                        "b1b1b1b1-b1b1-b1b1-b1b1-b1b1b1b1b1b1",
                        "c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1"
                    ],
                    "totalCount": 3
                }
            """

        when: "it is deserialized using a json deserializer"
        def locationServiceClusterResponse =
                new JsonHelper().MAPPER.readValue(locationServiceClusterResponseJson, LocationServiceClusterResponse.class)

        then: "it is deserialized as expected"
        locationServiceClusterResponse.clusters.size() == 3

        locationServiceClusterResponse.clusters.get(0) == "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1"
        locationServiceClusterResponse.clusters.get(1) == "b1b1b1b1-b1b1-b1b1-b1b1-b1b1b1b1b1b1"
        locationServiceClusterResponse.clusters.get(2) == "c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1"


    }
}
