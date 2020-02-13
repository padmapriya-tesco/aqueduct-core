package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.JsonHelper
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import com.tesco.aqueduct.pipe.api.OffsetEntity
import com.tesco.aqueduct.pipe.api.PipeState
import groovy.sql.Sql
import org.sqlite.SQLiteDataSource
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZoneId
import java.time.ZonedDateTime

import static com.tesco.aqueduct.pipe.api.OffsetName.*

class SQLiteStorageIntegrationSpec extends Specification {

    static final def connectionUrl = "jdbc:sqlite:aqueduct-pipe.db"
    static final def limit = 1000

    long batchSize = 1000
    long maxOverheadBatchSize = (Message.MAX_OVERHEAD_SIZE * limit) + batchSize
    private SQLiteStorage sqliteStorage

    ZonedDateTime currentUTCTime() {
        ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC"))
    }

    def message(long offset) {
        return message(offset, "some-type")
    }

    def message(long offset, String type, String data = "some-data") {
        return message(offset, "some-key", type, currentUTCTime(), data)
    }

    def message(long offset, String key, ZonedDateTime createdDateTime) {
        return message(offset, key, "some-type", createdDateTime)
    }

    def message(long offset, String key, String type, ZonedDateTime createdDateTime, String data = "some-data") {
        def messageForSizing = new Message(
            type,
            key,
            "text/plain",
            offset,
            createdDateTime,
            data
        )

        return new Message(
            type,
            key,
            "text/plain",
            offset,
            createdDateTime,
            data,
            JsonHelper.toJson(messageForSizing).length()
        )
    }

    def setup() {
        def sql = Sql.newInstance(connectionUrl)

        sql.execute("DROP TABLE IF EXISTS EVENT;")
        sql.execute("DROP TABLE IF EXISTS OFFSET;")
        sql.execute("DROP TABLE IF EXISTS PIPE_STATE;")

        sqliteStorage = new SQLiteStorage(successfulDataSource(), limit, 10, batchSize)

        sql.execute("INSERT INTO OFFSET (name, value) VALUES ('${GLOBAL_LATEST_OFFSET.toString()}',  3);")
    }

    def successfulDataSource() {
        def dataSource = new SQLiteDataSource()
        dataSource.setUrl(connectionUrl)

        return dataSource
    }

    def brokenDataSource() {
        def dataSource = new SQLiteDataSource()
        dataSource.setUrl("BROKEN CONNECTION")

        return dataSource
    }

    def 'events table gets created upon start up'() {
        given: 'a connection to the database is established'
        def sql = Sql.newInstance(connectionUrl)

        when: 'the SQLiteStorage class is instantiated'
        new SQLiteStorage(successfulDataSource(), limit, 10, batchSize)

        then: 'the events table is created'
        def tableExists = false
        sql.query("SELECT name FROM sqlite_master WHERE type='table' AND name='EVENT';", {
            it.next()
            tableExists = it.getString("name") == 'EVENT'
        })

        tableExists
    }

    def 'global latest offset table gets created upon start up'() {
        given: 'a connection to the database is established'
        def sql = Sql.newInstance(connectionUrl)

        when: 'the SQLiteStorage class is instantiated'
        new SQLiteStorage(successfulDataSource(), limit, 10, batchSize)

        then: 'the events table is created'
        def tableExists = false
        sql.query("SELECT name FROM sqlite_master WHERE type='table' AND name='OFFSET';", {
            it.next()
            tableExists = it.getString("name") == 'OFFSET'
        })

        tableExists
    }

    def 'pipe state table gets created upon start up'() {
        given: 'a connection to the database is established'
        def sql = Sql.newInstance(connectionUrl)

        when: 'the SQLiteStorage class is instantiated'
        new SQLiteStorage(successfulDataSource(), limit, 10, batchSize)

        then: 'the pipe state table is created'
        def tableExists = false
        sql.query("SELECT name FROM sqlite_master WHERE type='table' AND name='PIPE_STATE';", {
            it.next()
            tableExists = it.getString("name") == 'PIPE_STATE'
        })

        tableExists
    }

    def 'message is successfully written to the database'() {
        def offset = 1023L

        given: 'a message'
        Message message = message(offset)

        when: 'store the message to the database'
        sqliteStorage.write(message)

        then: 'the message is stored with no errors'
        notThrown(Exception)
    }

    def 'multiple messages are written to the database'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2)]

        and: 'a database table exists to be written to'
        def sql = Sql.newInstance(connectionUrl)

        when: 'these messages are passed to the data store controller'
        sqliteStorage.write(messages)

        then: 'all messages are written to the data store'
        def size = 0
        sql.query("SELECT COUNT(*) FROM EVENT", {
            it.next()
            size = it.getInt(1)
        })

        size == 2
    }

    def 'pipe state is successfully written to the database'() {
        given: 'the pipe state'
        def pipeState = PipeState.UP_TO_DATE

        and: 'a database connection'
        def sql = Sql.newInstance(connectionUrl)

        when: 'the pipe state is written'
        sqliteStorage.write(pipeState)

        then: 'the pipe state is stored in the pipe state table'
        def rows = sql.rows("SELECT value FROM PIPE_STATE WHERE name='pipe_state'")
        rows.get(0).get("value") == PipeState.UP_TO_DATE.toString()
    }

    def 'multiple writes of pipe state results in only one record in Pipe State table and value should reflect the last write'() {
        given: 'a database connection'
        def sql = Sql.newInstance(connectionUrl)

        when: 'the pipe state is written multiple times'
        sqliteStorage.write(PipeState.OUT_OF_DATE)
        sqliteStorage.write(PipeState.UP_TO_DATE)
        sqliteStorage.write(PipeState.OUT_OF_DATE)

        then: 'only one record exists in pipe state table'
        sql.rows("SELECT count(*) FROM PIPE_STATE").size() == 1

        and: 'value should be the last written state'
        def rows = sql.rows("SELECT value FROM PIPE_STATE WHERE name='pipe_state'")
        rows.get(0).get("value") == PipeState.OUT_OF_DATE.toString()
    }

    def 'newly stored message with offset is successfully retrieved from the database'() {
        def offset = 1023L

        given: 'a message'
        Message message = message(offset)

        when: 'store the message to the database'
        sqliteStorage.write(message)

        and: 'we retrieve the message from the database'
        MessageResults messageResults = sqliteStorage.read(null, offset, "locationUuid")
        Message retrievedMessage = messageResults.messages.get(0)

        then: 'the message retrieved should be what we saved'
        notThrown(Exception)
        message == retrievedMessage
    }

    def 'message with pipe state as UNKNOWN is returned when no state exists in the database'() {
        given: "no pipe state exist in the database"

        when: 'we retrieve the message from the database'
        MessageResults messageResults = sqliteStorage.read(null, 0, "locationUuid")

        then: 'the pipe states should be defaulted to OUT_OF_DATE'
        messageResults.pipeState == PipeState.UNKNOWN
    }

    def 'newly stored message with offset and pipe_state is successfully retrieved from the database'() {
        def offset = 1023L

        given: 'a message'
        Message message = message(offset)

        and: 'store the message to the database'
        sqliteStorage.write(message)

        and: 'store the pipe state to the database'
        sqliteStorage.write(PipeState.UP_TO_DATE)

        when: 'we retrieve the message from the database'
        MessageResults messageResults = sqliteStorage.read(null, offset, "locationUuid")
        Message retrievedMessage = messageResults.messages.get(0)

        then: 'the message retrieved should be what we saved'
        notThrown(Exception)
        message == retrievedMessage

        and: "pipe state should be what we saved"
        messageResults.pipeState == PipeState.UP_TO_DATE
    }

    def 'newly stored message with type is successfully retrieved from the database'() {
        def offset = 1023L

        given: 'a message'
        def expectedMessage = message(offset, 'my-new-type')
        Iterable<Message> messages = [expectedMessage, message(offset + 1L, 'my-other-type')]

        when: 'store multiple messages with different types to the database'
        sqliteStorage.write(messages)

        and: 'we retrieve the message from the database'
        MessageResults messageResults = sqliteStorage.read(['my-new-type'], offset, "locationUuid")
        Message retrievedMessage = messageResults.messages.get(0)

        then: 'the message retrieved should be what we saved'
        notThrown(Exception)
        messageResults.messages.size() == 1
        expectedMessage == retrievedMessage
    }

    def 'multiple messages can be read from the database starting from a given offset'() {
        given: 'multipe messages to be stored'
        def messages = [message(1), message(2), message(3), message(4)]

        and: 'these messages stored'
        sqliteStorage.write(messages)

        when: 'all messages with offset are read'
        MessageResults messageResults = sqliteStorage.read(null, 2, "locationUuid")

        then: 'multiple messages are retrieved'
        messageResults.messages.size() == 3
    }

    def 'limit on amount of multiple messages returned returns only as many messages as limit defines'() {
        given: 'multipe messages to be stored'
        def messages = [message(1), message(2), message(3), message(4)]

        and: 'a data store controller exists'
        def sqliteStorage = new SQLiteStorage(successfulDataSource(), 3, 10, batchSize)

        and: 'these messages stored'
        sqliteStorage.write(messages)

        when: 'all messages with offset are read'
        MessageResults messageResults = sqliteStorage.read(null, 2, "locationUuid")

        then: 'multiple messages are retrieved'
        messageResults.messages.size() == 3
    }

    def 'retrieves the latest offset'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2), message(3), message(4)]

        and: 'these messages are stored'
        sqliteStorage.write(messages)

        when: 'requesting the latest offset with no tags'
        def latestOffset = sqliteStorage.getLatestOffsetMatching([])

        then: 'the latest offset is returned'
        latestOffset == 4
    }

    def 'retrieves the latest offset for a given type'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2, 'my-new-type'), message(3), message(4)]

        and: 'these messages are stored'
        sqliteStorage.write(messages)

        when: 'requesting the latest offset with no tags'
        def latestOffset = sqliteStorage.getLatestOffsetMatching(['my-new-type'])

        then: 'the latest offset is returned'
        latestOffset == 2
    }

    def "the messages returned are no larger than the maximum batch size"() {
        given: "a size of each message is calculated so that 3 messages are just larger than the max overhead batch size"
        int messageSize = Double.valueOf(maxOverheadBatchSize / 3).intValue() + 1

        and: "messages are created"
        def msg1 = message(1, "type-1", "x"*messageSize)
        def msg2 = message(2, "type-1", "x"*messageSize)
        def msg3 = message(3, "type-1", "x"*messageSize)

        and: "they are inserted into the integrated database"
        sqliteStorage.write(msg1)
        sqliteStorage.write(msg2)
        sqliteStorage.write(msg3)

        when: "reading from the database"
        MessageResults result = sqliteStorage.read(["type-1"], 0, "locationUuid")

        then: "messages that are returned are no larger than the maximum batch size"
        result.messages.size() == 2
    }

    def 'read all messages of multiple specified types after the given offset'() {
        given: 'multiple messages to be stored'
        def messages = [message(1),
                        message(2, 'type-1'),
                        message(3, 'type-2'),
                        message(4)]

        and: 'these messages are stored'
        sqliteStorage.write(messages)

        when: 'reading the messages of multiple specified types from a given offset'
        def receivedMessages = sqliteStorage.read(['type-1', 'type-2'], 1, "locationUuid")

        then: 'only the messages matching the multiple specified types from the given offset are returned'
        receivedMessages.messages.size() == 2
        receivedMessages.messages[0] == messages.get(1)
        receivedMessages.messages[1] == messages.get(2)
    }

    def 'retrieves the latest offset for multiple specified types'() {
        given: 'multiple messages to be stored'
        def messages = [
            message(1),
            message(2, 'type-1'),
            message(3, 'type-2'),
            message(4)
        ]

        and: 'these messages are stored'
        sqliteStorage.write(messages)

        when: 'requesting the latest offset with multiple specified types'
        def latestOffset = sqliteStorage.getLatestOffsetMatching(['type-1', 'type-2'])

        then: 'the latest offset for the messages with one of the types is returned'
        latestOffset == 3
    }

    def 'retrieves the global latest offset'() {
        given: 'offset table exists with globalLatestOffset'
        // setup creates the table and populates globalLatestOffset

        when: 'performing a read into the database'
        def messageResults = sqliteStorage.read(['type-1'], 1, "locationUuid")

        then: 'the latest offset for the messages with one of the types is returned'
        messageResults.getGlobalLatestOffset() == OptionalLong.of(3)
    }

    def 'retrieves the global latest offset as empty when it does not exist'() {
        given: 'offset table exists with no globalLatestOffset'
        def sql = Sql.newInstance(connectionUrl)
        sql.execute("DROP TABLE IF EXISTS OFFSET;")
        sqliteStorage = new SQLiteStorage(successfulDataSource(), limit, 10, batchSize)

        when: 'performing a read into the database'
        def messageResults = sqliteStorage.read(['type-1'], 1, "locationUuid")

        then: 'the latest offset for the messages with one of the types is returned'
        messageResults.getGlobalLatestOffset() == OptionalLong.empty()
    }

    def 'messages are ready from the database with the correct retry after'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2), message(3), message(4), message(5)]

        and: 'a data store controller exists'
        def sqliteStorage = new SQLiteStorage(successfulDataSource(), 5, 10, batchSize)

        and: 'these messages stored'
        sqliteStorage.write(messages)

        when: 'all messages with offset are read'
        MessageResults messageResults = sqliteStorage.read(null, 1, "locationUuid")

        then: 'the retry after is 0'
        messageResults.retryAfterSeconds == 0
    }

    def 'throws an exception if there is a problem with the database connection on startup'() {
        when: 'a data store controller exists with a broken connector url'
        new SQLiteStorage(brokenDataSource(), limit, 10, batchSize)

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'All duplicate messages are compacted for whole data store'() {
        given: 'an existing data store with duplicate messages for the same key'
        def messages = [
                message(1, "A", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(2, "B", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(3, "A", ZonedDateTime.parse("2000-12-01T10:00:00Z"))
        ]
        sqliteStorage.write(messages)

        when: 'compaction is run on the whole data store'
        sqliteStorage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = sqliteStorage.read(null, 1, "locationUuid")
        List<Message> retrievedMessages = messageResults.messages

        then: 'duplicate messages are deleted'
        retrievedMessages.size() == 2

        and: 'the correct compacted message list is returned in the message results'
        messageResults.messages*.offset*.intValue() == [2, 3]
        messageResults.messages*.key == ["B", "A"]
    }

    def 'All duplicate messages are compacted to a given offset with 3 duplicates'() {
        given: 'an existing data store with duplicate messages for the same key'
        def messages = [
                message(1, "A", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(2, "A", ZonedDateTime.parse("2000-12-03T10:00:00Z")),
                message(3, "A", ZonedDateTime.parse("2000-12-03T10:00:00Z")),
                message(4, "B", ZonedDateTime.parse("2000-12-03T10:00:00Z"))
        ]
        sqliteStorage.write(messages)

        when: 'compaction is run up to the timestamp of offset 1'
        sqliteStorage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = sqliteStorage.read(null, 1, "locationUuid")

        then: 'duplicate messages are not deleted as they are beyond the threshold'
        messageResults.messages.size() == 4
        messageResults.messages*.offset*.intValue() == [1, 2, 3, 4]
        messageResults.messages*.key == ["A", "A", "A", "B"]
    }

    def 'All duplicate messages are compacted to a given offset, complex case'() {
        given: 'an existing data store with duplicate messages for the same key'
        def messages = [
                message(1, "A", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(2, "B", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(3, "C", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(4, "C", ZonedDateTime.parse("2000-12-01T10:00:00Z")),
                message(5, "A", ZonedDateTime.parse("2000-12-03T10:00:00Z")),
                message(6, "B", ZonedDateTime.parse("2000-12-03T10:00:00Z")),
                message(7, "B", ZonedDateTime.parse("2000-12-03T10:00:00Z")),
                message(8, "D", ZonedDateTime.parse("2000-12-03T10:00:00Z"))
        ]
        sqliteStorage.write(messages)

        when: 'compaction is run up to the timestamp of offset 4'
        sqliteStorage.compactUpTo(ZonedDateTime.parse("2000-12-02T10:00:00Z"))

        and: 'all messages are requested'
        MessageResults messageResults = sqliteStorage.read(null, 1, "locationUuid")

        then: 'duplicate messages are deleted that are within the threshold'
        messageResults.messages.size() == 7
        messageResults.messages*.offset*.intValue() == [1, 2, 4, 5, 6, 7, 8]
        messageResults.messages*.key == ["A", "B", "C", "A", "B", "B", "D"]
    }

    def 'messages and offset are deleted when deleteAllMessages is called'() {
        given: 'multiple messages to be stored'
        def messages = [message(1), message(2)]

        and: 'offset exist already' // from setup

        and: 'a database table exists to be written to'
        def sql = Sql.newInstance(connectionUrl)

        and: 'these messages are written'
        this.sqliteStorage.write(messages)

        and: 'all messages are written to the data store'
        def messagesFirstSize = 0
        sql.query("SELECT COUNT(*) FROM EVENT", {
            it.next()
            messagesFirstSize = it.getInt(1)
        })

        assert messagesFirstSize == 2

        and: 'offset exists in the OFFSET table'
        def offsetFirstSize = 0
        sql.query("SELECT COUNT(*) FROM OFFSET", {
            it.next()
            offsetFirstSize = it.getInt(1)
        })

        assert offsetFirstSize == 1

        when:
        this.sqliteStorage.deleteAll()

        then: 'no messages exists in EVENT'
        def messagesSecondSize = 0
        sql.query("SELECT COUNT(*) FROM EVENT", {
            it.next()
            messagesSecondSize = it.getInt(1)
        })

        messagesSecondSize == 0

        and: 'no offset exists in the table'
        def offsetSecondSize = 0
        sql.query("SELECT COUNT(*) FROM OFFSET", {
            it.next()
            offsetSecondSize = it.getInt(1)
        })

        offsetSecondSize == 0
    }

    @Unroll
    def 'offset is written into the OFFSET table'() {
        given: "an offset to be written into the database"
        OffsetEntity offset = new OffsetEntity(offsetName, offsetValue)

        and: 'a database table exists to be written to'
        def sql = Sql.newInstance(connectionUrl)

        when: "the offset is written"
        sqliteStorage.write(offset)

        then: "the offset is stored into the database"
        OffsetEntity result
        sql.query("SELECT name, value FROM OFFSET WHERE name = ${offsetName.toString()}", {
            it.next()
            result = new OffsetEntity(valueOf(it.getString(1)), OptionalLong.of(it.getLong(2)))
        })

        result == offset

        where:
        offsetName           | offsetValue
        GLOBAL_LATEST_OFFSET | OptionalLong.of(1L)
        LOCAL_LATEST_OFFSET  | OptionalLong.of(2L)
    }

    def 'offset is updated when already present in OFFSET table'() {
        given: "an offset to be written into the database"
        def name = GLOBAL_LATEST_OFFSET
        OffsetEntity offset = new OffsetEntity(name, OptionalLong.of(1113))

        and: 'a database table exists to be written to'
        def sql = Sql.newInstance(connectionUrl)

        when: "the offset is written"
        sqliteStorage.write(offset)

        and: "the offset is updated"
        OffsetEntity updatedOffset = new OffsetEntity(name, OptionalLong.of(1114))
        sqliteStorage.write(updatedOffset)

        then: "the offset is stored into the database"
        OffsetEntity result
        sql.query("SELECT name, value FROM OFFSET WHERE name = ${name.toString()}", {
            it.next()
            result = new OffsetEntity(valueOf(it.getString(1)), OptionalLong.of(it.getLong(2)))
        })

        result == updatedOffset
    }

    @Unroll
    def 'the latest offset entity is returned from the db'() {
        given: "the offset entity exists in the offset table"
        def sql = Sql.newInstance(connectionUrl)
        sql.execute("INSERT INTO OFFSET (name, value) VALUES (${offsetName.toString()}, ${offsetValue.asLong})" +
                " ON CONFLICT(name) DO UPDATE SET VALUE = ${offsetValue.asLong};")

        when: "we retrieve the offset"
        def result = sqliteStorage.getLatestOffset(offsetName)

        then: "the correct offset value is returned"
        result == offsetValue

        where:
        offsetName           | offsetValue
        GLOBAL_LATEST_OFFSET | OptionalLong.of(1L)
        LOCAL_LATEST_OFFSET  | OptionalLong.of(2L)
    }
}
