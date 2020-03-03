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
                        {
                            "id": "c8559b7d-5a46-1234-b692-739725caf570",
                            "name": "placeholder name",
                            "origin": "placeholder_origin"
                        },
                        {
                            "id": "041fece7-1234-42c6-9e76-72a43f599498",
                            "name": "placeholder name",
                            "origin": "placeholder_origin"
                        },
                        {
                            "id": "417bda80-bfba-412c-1234-c35d68530342",
                            "name": "placeholder name",
                            "origin": "placeholder_origin"
                        }
                    ],
                    "totalCount": 3
                }
            """

        when: "it is deserialized using a json deserializer"
        def locationServiceClusterResponse =
                new JsonHelper().MAPPER.readValue(locationServiceClusterResponseJson, LocationServiceClusterResponse.class)

        then: "it is deserialized as expected"
        locationServiceClusterResponse.clusters.size() == 3

        locationServiceClusterResponse.clusters.get(0).id == "c8559b7d-5a46-451e-b692-739725caf570"
        locationServiceClusterResponse.clusters.get(1).id == "041fece7-aa93-42c6-9e76-72a43f599498"
        locationServiceClusterResponse.clusters.get(2).id == "417bda80-bfba-412c-9b52-c35d68530342"


    }
}
