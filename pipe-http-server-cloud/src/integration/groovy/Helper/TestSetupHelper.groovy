package Helper

import groovy.sql.Sql

import javax.sql.DataSource

class TestSetupHelper {

    static Sql setupPostgres(DataSource dataSource, Sql sql) {
        sql = new Sql(dataSource.connection)
        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS CLUSTERS;
          
        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1
        );
        
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');
        """)

        return sql
    }
}
