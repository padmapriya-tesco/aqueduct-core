package com.tesco.aqueduct.registry.model

import com.tesco.aqueduct.registry.VersionChangedException
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement

class PostgresNodeGroupSpec extends Specification {
    def "can update groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we update the node group"
        group.persist(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("UPDATE")
    }

    def "If an update fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we update the node group"
        group.persist(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }

    def "can delete groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we delete the node group"
        group.delete(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("DELETE")
    }

    def "If a delete fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we delete the node group"
        group.delete(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }

    def "can insert groups"() {
        String statement
        given: "a SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> {
                statement = it
                return Mock(PreparedStatement) {
                    1 * executeUpdate() >> 1
                }
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id")
        group.add(n1, new URL("http://cloud"))
        when: "we insert the node group"
        group.persist(connection)
        then: "a statement has been run against the SQL connection"
        statement.contains("INSERT")
    }

    def "If an insert fails, then throw an exception"() {
        given: "a failing SQL connection"
        def connection = Mock(Connection) {
            1* prepareStatement(_) >> Mock(PreparedStatement) {
                1* executeUpdate() >> 0
            }
        }
        and: "a PostgresNodeGroup"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .build()
        def group = new PostgresNodeGroup("test-id", 99, [n1])
        when: "we insert the node group"
        group.persist(connection)
        then: "a Version Changed Exception is thrown"
        thrown(VersionChangedException)
    }
}
