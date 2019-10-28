package com.tesco.aqueduct.registry.model

import spock.lang.Specification

class BootstrapRequestSpec extends Specification {
    def "Saving request for multiple tills"() {
        given: "A Bootstrap request for more than one till"
        def testRequest = new BootstrapRequest(["host-1", "host-2"], BootstrapType.PIPE_AND_PROVIDER)
        and: "A mock TillStorage object"
        def tillStorageMock = Mock(TillStorage)
        when: "Saving the bootstrap request"
        testRequest.save(tillStorageMock)
        then: "tillStorageMock has been called with all hosts"
        2* tillStorageMock.save(_ as Till)
    }

    def "Saving request for Zero tills"() {
        given: "A Bootstrap request for no tills"
        def testRequest = new BootstrapRequest([], BootstrapType.PIPE_AND_PROVIDER)
        and: "A mock TillStorage object"
        def tillStorageMock = Mock(TillStorage)
        when: "Saving the bootstrap request"
        testRequest.save(tillStorageMock)
        then: "tillStorageMock has been called with all hosts"
        0* tillStorageMock.save(_ as Till)
    }
}
