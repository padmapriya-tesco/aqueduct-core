package com.tesco.aqueduct.registry.postgres

import com.tesco.aqueduct.registry.model.Node
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement

class PostgresNodeGroupSpec extends Specification {
    def "can update groups"() {
        String statement

        given: "a SQL connection"
        def connection = Mock(Connection) {
            1 * prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }

        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .pipe(["v":"1.0"])
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])

        when: "we update the node group"
        group.persist(connection)

        then: "a statement has been run against the SQL connection"
        statement.contains("UPDATE")
    }

    def "can delete groups"() {
        String statement

        given: "a SQL connection"
        def connection = Mock(Connection) {
            1 * prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }

        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .pipe(["v":"1.0"])
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])

        when: "we delete the node group"
        group.delete(connection)

        then: "a statement has been run against the SQL connection"
        statement.contains("DELETE")
    }

    def "can insert groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1 * prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }

        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .pipe(["v":"1.0"])
            .build()

        def group = new PostgresNodeGroup("test-id")
        group.upsert(n1, new URL("http://cloud"))

        when: "we insert the node group"
        group.persist(connection)

        then: "a statement has been run against the SQL connection"
        statement.contains("INSERT")
    }
}
