package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.*
import com.tesco.aqueduct.pipe.http.PipeStateProvider
import spock.lang.Specification

class DistributedStorageSpec extends Specification {
    def messageReader = Mock(MessageReader)
    def pipeStateProvider = Mock(PipeStateProvider)
    def messageWriter = Mock(MessageWriter)
    def distributedStorage = new DistributedStorage(messageReader, messageWriter, pipeStateProvider)

    def "read messages should return results with messages and pipe state from local storage"() {
        given: "a messages reader"
        messageReader.read([], 100, "someLocationUuid") >> new MessageEntity([], 1L, OptionalLong.of(100L))

        and: "a pipe state provider"
        pipeStateProvider.getState() >> PipeState.UP_TO_DATE

        when: "we read messages from distributed storage"
        def messageResults = distributedStorage.read([], 100, "someLocationUuid")

        then:
        messageResults.messages == []
        messageResults.pipeState == PipeState.UP_TO_DATE
        messageResults.globalLatestOffset == OptionalLong.of(100L)
    }

    def "get latest offset matching should delegate to message reader"() {
        when: "a message is written"
        distributedStorage.getLatestOffsetMatching([])

        then: "message writer calls write"
        1 * messageReader.getLatestOffsetMatching(_)
    }

    def "write messages should delegate to message writer"() {
        when: "a message is written"
        distributedStorage.write(Mock(Message))

        then: "message writer calls write"
        1 * messageWriter.write(_)
    }

    def "write offset entity should delegate to message writer"() {
        given: "a message reader, message writer and pipe state provider"
        def offsetEntity = Mock(OffsetEntity)

        when: "a message is written"
        distributedStorage.write(offsetEntity)

        then: "message writer calls write"
        1 * messageWriter.write(offsetEntity)
    }

    def "write pipe state should delegate to pipe state provider"() {
        when: "a message is written"
        distributedStorage.write(PipeState.UP_TO_DATE)

        then: "message writer calls write"
        1 * pipeStateProvider.setState(PipeState.UP_TO_DATE)
    }

    def "delete all should delegate to message writer"() {
        when: "delete all is called"
        distributedStorage.deleteAll()

        then: "message writer calls delete all"
        1 * messageWriter.deleteAll()
    }
}
