package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageStorage
import com.tesco.aqueduct.pipe.storage.sqlite.TimedMessageStorage
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

class TimedMessageStorageSpec extends Specification {
    final long OFFSET = 1
    final Message MOCK_MESSAGE = Mock(Message)
    final Map<String, List<String>> MESSAGE_TAGS = Collections.emptyMap()
    final def METER_REGISTRY = Spy(SimpleMeterRegistry)

    def setup() {
    }

    def "read events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the read method"
        timedStorage.read(MESSAGE_TAGS, OFFSET)

        then: "the read method is called on the underlying storage"
        1 * mockedStorage.read(MESSAGE_TAGS,OFFSET)
    }

    def "write message events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the write method"
        timedStorage.write(MOCK_MESSAGE)

        then: "the write method is called on the underlying storage"
        1 * mockedStorage.write(MOCK_MESSAGE)
    }

    def "write message list events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the write list method"
        timedStorage.write([MOCK_MESSAGE])

        then: "the write list method is called on the underlying storage"
        1 * mockedStorage.write([MOCK_MESSAGE])
    }

    def "get latest offset events are timed"() {
        given: "we have an instance of TimedMessageStorage"
        def mockedStorage = Mock(MessageStorage)
        def timedStorage = new TimedMessageStorage(mockedStorage, METER_REGISTRY)

        when: "we call the get latest offset method"
        timedStorage.getLatestOffsetMatching(MESSAGE_TAGS)

        then: "the get latest method is called on the underlying storage"
        1 * mockedStorage.getLatestOffsetMatching(MESSAGE_TAGS)
    }
}
