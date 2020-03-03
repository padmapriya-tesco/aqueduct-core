package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.pipe.api.PipeState
import spock.lang.Specification

class CloudPipeStateProviderTest extends Specification {
    def "cloud is always up to date"() {
        expect:
        new CloudPipeStateProvider().getState(Mock(MessageReader)) == PipeState.UP_TO_DATE
    }
}
