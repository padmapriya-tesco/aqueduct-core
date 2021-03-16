package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import groovy.sql.Sql
import groovy.transform.NamedVariant
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

import static java.sql.DriverManager.getConnection

class OffsetFetcherIntegrationSpec extends Specification {

    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql

    private OffsetFetcher offsetFetcher

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
        offsetFetcher = new OffsetFetcher(10)
    }

    def "Max offset is fetched from events table when global latest offset does not exist in offsets table"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        def currentTime = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
        def createdTime = currentTime - 11
        insert(message(3, createdTime))
        insert(message(4, currentTime))

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 3
    }

    def "offset is fetched from offsets table when it exist there and is less than max offset"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        def currentTime = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
        def createdTime = currentTime - 11
        insert(message(3, createdTime))
        insert(message(4, currentTime))

        and: "global latest offset exists in offsets table"
        sql.execute("INSERT INTO offsets (name, value) values('GLOBAL_LATEST_OFFSET', 2)")

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 2
    }

    def "max offset is fetched from events table when it exist in offsets table and is higher than max offset"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists prior to threshold time limit"
        def currentTime = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
        def createdTime = currentTime - 11
        insert(message(3, createdTime))
        insert(message(4, currentTime))

        and: "global latest offset exists in offsets table"
        sql.execute("INSERT INTO offsets (name, value) values('GLOBAL_LATEST_OFFSET', 4)")

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 3
    }

    def "offset is fetched from offsets table when exists and there are no messages in events within the given threshold"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists outside of threshold time limit"
        def currentTime = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
        def createdTime = currentTime - 11
        insert(message(1, createdTime))
        insert(message(2, createdTime))

        and: "global latest offset exists in offsets table"
        sql.execute("INSERT INTO offsets (name, value) values('GLOBAL_LATEST_OFFSET', 2)")

        when:
        def globalLatestOffset = offsetFetcher.getGlobalLatestOffset(connection)

        then:
        globalLatestOffset == 2
    }

    def "max offset is fetched when offsets table is empty and there are no messages in events within the given threshold"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists outside of threshold time limit"
        def currentTime = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))
        def createdTime = currentTime - 11
        insert(message(1, createdTime))
        insert(message(2, createdTime))

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
            Long offset, ZonedDateTime createdTime=time
    ) {
        new Message(
            "type",
            "key",
            "contentType",
            offset,
            createdTime,
            "data"
        )
    }
}
