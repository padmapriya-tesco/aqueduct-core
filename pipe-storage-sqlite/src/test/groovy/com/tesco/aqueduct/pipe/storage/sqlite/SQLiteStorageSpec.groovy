package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.OffsetEntity
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.DriverManager
import java.sql.SQLException
import java.time.ZoneId
import java.time.ZonedDateTime

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET

class SQLiteStorageSpec extends Specification {

    static final def connectionUrl = "jdbc:sqlite:aqueduct-pipe.db"
    static final def limit = 1000

    long batchSize = 1000

    def message(long offset, String type) {
        def timeNow = ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC"))
        return new Message(
                type,
                "some-key",
                "text/plain",
                offset,
                timeNow,
                null,
                "some-data"
        )
    }

    def dataSource
    SQLiteStorage sqliteStorage

    def setup() {
        dataSource = Mock(DataSource)

        dataSource.getConnection() >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            {throw new SQLException()}

        sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)
    }

    def message(long offset) {
        return message(offset, "some-type")
    }

    def 'throws an exception if a problem with the database arises when reading messages'() {
        given: 'a data store controller exists with a broken connection url'

        when: 'messages are requested to be read from a given offset'
        sqliteStorage.read([], 0)

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing messages'() {
        given: 'a data store controller exists with a broken connection url'

        when: 'the latest offset is requested'
        sqliteStorage.write(message(1))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing latest offset'() {
        given: 'a data store controller exists with a broken connection url'

        when: 'the latest offset is written'
        sqliteStorage.write(new OffsetEntity("someOffsetName", OptionalLong.of(100)))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if no offset value provided when writing latest offset'() {
        given: 'a data store controller and sqlite storage exists'
        def dataSource = Mock(DataSource)
        dataSource.getConnection() >>> [
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl),
            DriverManager.getConnection(connectionUrl)
        ]

        def sqliteStorage = new SQLiteStorage(dataSource, limit, 10, batchSize)

        when: 'the latest offset is written with empty value'
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.empty()))

        then: 'no such element exception is thrown'
        thrown(NoSuchElementException)
    }

    def 'retry read time limit should be activated only when the amount of received messages is 0'() {
        given: 'a data store controller'
        def retryAfter = 10
        def dataSource = Mock(DataSource)
        dataSource.getConnection() >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            DriverManager.getConnection(connectionUrl) >>
            { throw new SQLException() }

        def sqliteStorage = new SQLiteStorage(dataSource, testLimit, retryAfter, batchSize)

        when: 'the retry after is calculated'
        def actualRetryAfter = sqliteStorage.calculateRetryAfter(messageCount)

        then: 'the calculated retry after is 0 if more than 0 messages were returned'
        actualRetryAfter == expectedRetryAfter

        where:
        testLimit | messageCount | expectedRetryAfter
        100       | 50           | 0
        100       | 0            | 10
        100       | 99           | 0
        99        | 98           | 0
        99        | 0            | 10
        100       | 101          | 0
        101       | 102          | 0
        100       | 100          | 0
        99        | 100          | 0
        0         | 1            | 0
    }
}
