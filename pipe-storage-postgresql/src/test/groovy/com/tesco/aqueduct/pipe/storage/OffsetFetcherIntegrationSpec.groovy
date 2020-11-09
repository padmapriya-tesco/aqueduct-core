package com.tesco.aqueduct.pipe.storage

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

@MicronautTest
class OffsetFetcherIntegrationSpec extends Specification {

    def "Offset is cached once fetched from db storage"() {
        given:
        OffsetFetcher offsetFetcher = new OffsetFetcher(5)

        and:
        def connection = Mock(Connection)
        def preparedStatement = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)

        when:
        def globalLatestOffset1 = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        1 * connection.prepareStatement(*_) >> preparedStatement
        1 * preparedStatement.executeQuery() >> resultSet
        1 * resultSet.getLong("last_offset") >> 10

        globalLatestOffset1 == 10

        when:
        def globalLatestOffset2 = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        0 * connection.prepareStatement(*_) >> preparedStatement
        globalLatestOffset2 == 10
    }
}
