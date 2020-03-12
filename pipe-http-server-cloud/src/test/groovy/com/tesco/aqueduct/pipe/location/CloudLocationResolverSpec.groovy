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

    def "Illegal argument error when location uuid passed is null"() {
        given:
        def locationUuid = null

        and:
        def locationServiceClient = Mock(LocationServiceClient)

        when:
        new CloudLocationResolver(locationServiceClient).resolve(locationUuid)

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when location client is null"() {
        given:
        def locationServiceClient = null

        when:
        new CloudLocationResolver(locationServiceClient)

        then:
        thrown(IllegalArgumentException)
    }
}
