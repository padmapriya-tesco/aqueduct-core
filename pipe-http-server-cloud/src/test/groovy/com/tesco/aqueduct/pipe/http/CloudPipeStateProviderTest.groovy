package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.api.Reader
import spock.lang.Specification

class CloudPipeStateProviderTest extends Specification {
    def "cloud is always up to date"() {
        given: "a reader mock"
        def reader = Mock(Reader) {
            getOffset(_) >> OptionalLong.of(1L)
        }

        when: "getting pipe state"
        def pipeState = new CloudPipeStateProvider().getState([], reader)

        then: "pipe is up to date"
        pipeState == new PipeStateResponse(true, 1L)
    }
}
