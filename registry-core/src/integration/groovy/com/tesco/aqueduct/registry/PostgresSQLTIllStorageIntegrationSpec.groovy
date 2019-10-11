package com.tesco.aqueduct.registry

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.registry.model.BootstrapType
import groovy.sql.Sql
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.DriverManager

class PostgresSQLTIllStorageIntegrationSpec extends Specification {

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    DataSource dataSource
    TillStorage tillStorage

    def setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
            DROP TABLE IF EXISTS tills;
            
            CREATE TABLE tills(
                host_id VARCHAR PRIMARY KEY NOT NULL,
                bootstrap_requested DATE NOT NULL,
                bootstrap_type VARCHAR NOT NULL,
                bootstrap_received DATE
            );
        """)

        tillStorage = new PostgreSQLTillStorage(dataSource)
    }

    def "When a till is updated an entry is added to the database"() {
        given: "a postgres till storage"

        when: "bootstrap type is requested"
        tillStorage.updateTill("host-id", BootstrapType.PROVIDER)

        then: "data store contains the correct entry"
        def rows = sql.rows("SELECT * FROM tills;")
        def dataSet = sql.dataSet("tills")

        rows.get(0).getProperty("host_id") == "host-id"
        rows.get(0).getProperty("bootstrap_type") == "PROVIDER"
    }
}
