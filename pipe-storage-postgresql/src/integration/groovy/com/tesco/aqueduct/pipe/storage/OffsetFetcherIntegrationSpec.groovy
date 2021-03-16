package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import groovy.sql.Sql
import groovy.transform.NamedVariant
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
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

import static java.sql.DriverManager.getConnection

@MicronautTest
@Property(name="micronaut.caches.latest-offset-cache.expire-after-write", value="1h")
@Property(name="persistence.read.read-delay-seconds", value = "0")
class OffsetFetcherIntegrationSpec extends Specification {

    @Inject
    private ApplicationContext applicationContext

    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql

    void setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS OFFSETS;

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
        
          CREATE TABLE OFFSETS(
            name VARCHAR PRIMARY KEY NOT NULL,
            value BIGINT NOT NULL
          );
        """)
    }

    def "Offset is cached once fetched from db storage"() {
        given:
        def offsetFetcher = applicationContext.getBean(OffsetFetcher)

        and:
        def connection = Mock(Connection)
        def anotherConnection = Mock(Connection)
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
        def globalLatestOffset2 = offsetFetcher.getGlobalLatestOffset(anotherConnection)

        then:
        0 * anotherConnection.prepareStatement(*_)
        globalLatestOffset2 == 10
    }

    def "Max offset is fetched from events table when global latest offset does not exist in offsets table"() {
        given:
        def offsetFetcher = applicationContext.getBean(OffsetFetcher)

        and: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        insert(message(1))
        insert(message(2))
        insert(message(3))

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 3
    }

    def "offset is fetched from offsets table when it exist there and is less than max offset"() {
        given:
        def offsetFetcher = applicationContext.getBean(OffsetFetcher)

        and: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        insert(message(1))
        insert(message(2))
        insert(message(3))

        and: "global latest offset exists in offsets table"
        sql.execute("INSERT INTO offsets (name, value) values('GLOBAL_LATEST_OFFSET', 2)")

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 2
    }

    void insert(Message msg, Long clusterId=1, int messageSize=0, def time = Timestamp.valueOf(msg.created.toLocalDateTime())) {
        if (msg.offset == null) {
            sql.execute(
                    "INSERT INTO EVENTS(msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?);",
                    msg.key, msg.contentType, msg.type, time, msg.data, messageSize, clusterId
            )
        } else {
            sql.execute(
                    "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
                    msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, messageSize, clusterId
            )
        }
    }

    static ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))

    @NamedVariant
    Message message(
            Long offset
    ) {
        new Message(
            "type",
            "key",
            "contentType",
            offset,
            time,
            "data"
        )
    }
}
