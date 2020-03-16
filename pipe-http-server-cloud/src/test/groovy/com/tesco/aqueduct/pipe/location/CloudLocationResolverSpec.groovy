package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.Cluster
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
}
