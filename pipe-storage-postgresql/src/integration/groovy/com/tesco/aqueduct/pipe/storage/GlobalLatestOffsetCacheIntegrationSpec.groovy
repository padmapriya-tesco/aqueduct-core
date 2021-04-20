package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.inject.Inject
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDateTime

import static java.sql.DriverManager.getConnection

@MicronautTest(rebuildContext = true)
@Property(name="micronaut.caches.latest-offset-cache.expire-after-write", value="1h")
class GlobalLatestOffsetCacheIntegrationSpec extends Specification {

    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql

    @Inject
    private ApplicationContext applicationContext

    private GlobalLatestOffsetCache globalLatestOffsetCache

    void setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;

        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1,
            time_to_live TIMESTAMP NULL
        );
        """)

        globalLatestOffsetCache = applicationContext.getBean(GlobalLatestOffsetCache)
    }

    def "Offset is cached once fetched from db storage"() {
        given:
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and:
        insertMessage(100)

        when:
        globalLatestOffsetCache.getGlobalLatestOffset(connection) == 100

        and:
        insertMessage(101)

        then:
        globalLatestOffsetCache.getGlobalLatestOffset(connection) == 100

    }

    def "max offset is fetched from events table when not looking in cache"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        insertMessage(3)
        insertMessage(4)

        when:
        def globalLatestOffset = globalLatestOffsetCache.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 4
    }

    def "returns 0 if nothing is returned from getGlobalLatestOffset query"() {
        given:
        def connection = Mock(Connection)
        def preparedStatement = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)

        when:
        def globalLatestOffset = globalLatestOffsetCache.getGlobalLatestOffset(connection)

        then:
        1 * connection.prepareStatement(*_) >> preparedStatement
        1 * preparedStatement.executeQuery() >> resultSet
        1 * resultSet.next() >> false

        globalLatestOffset == 0
    }

    void insertMessage(Long offset) {
        sql.execute(
            "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
            offset, "key", "contentType", "type", Timestamp.valueOf(LocalDateTime.now()), "data", 10, 1
        )
    }
}
