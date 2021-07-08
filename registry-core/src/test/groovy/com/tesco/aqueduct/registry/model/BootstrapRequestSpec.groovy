package com.tesco.aqueduct.registry.model

import spock.lang.Specification

class BootstrapRequestSpec extends Specification {
    def "Saving request for multiple nodes"() {
        given: "A Bootstrap request for more than one node"
        def testRequest = new BootstrapRequest([], ["host-1", "host-2"], BootstrapType.PIPE_AND_PROVIDER)

        and: "A mock node request storage object"
        def nodeRequestStorage = Mock(NodeRequestStorage)

        and: "A mock node registry object"
        def nodeRegistry = Mock(NodeRegistry)

        when: "Saving the bootstrap request"
        testRequest.save(nodeRequestStorage, nodeRegistry)

        then: "node request storage has been called with all hosts"
        2* nodeRequestStorage.save(_ as NodeRequest)
    }

    def "Saving request for Zero nodes"() {
        given: "A Bootstrap request for no nodes"
        def testRequest = new BootstrapRequest([], [], BootstrapType.PIPE_AND_PROVIDER)

        and: "A mock node request storage object"
        def nodeRequestStorage = Mock(NodeRequestStorage)

        and: "A mock node registry object"
        def nodeRegistry = Mock(NodeRegistry)

        when: "Saving the bootstrap request"
        testRequest.save(nodeRequestStorage, nodeRegistry)

        then: "node request storage has been called with all hosts"
        0* nodeRequestStorage.save(_ as NodeRequest)
    }

    def "Saving request for multiple nodes and locations"() {
        given: "A Bootstrap request for more than one node"
        def testRequest = new BootstrapRequest(["locationA", "locationB"], ["host-1", "host-2"], BootstrapType.PIPE_AND_PROVIDER)

        and: "A mock node request storage object"
        def nodeRequestStorage = Mock(NodeRequestStorage)

        and: "A mock node registry object"
        def nodeRegistry = Mock(NodeRegistry)
        nodeRegistry.getNodeHostsForGroups(["locationA", "locationB"]) >> ["host-3", "host-4", "host-5"]

        when: "Saving the bootstrap request"
        testRequest.save(nodeRequestStorage, nodeRegistry)

        then: "node request storage has been called with all hosts"
        5* nodeRequestStorage.save(_ as NodeRequest)
    }
}
