package com.tesco.aqueduct.registry.model

import com.tesco.aqueduct.registry.VersionChangedException
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement

class PostgresNodeGroupSpec extends Specification {
	def "can update groups"() {
		given: "a SQL connection"
		def connection = Mock(Connection) {
			1* prepareStatement(_) >> Mock(PreparedStatement) {
				1* executeUpdate() >> 1
			}
		}
		and: "a PostgresNodeGroup"
		Node n1 = Node.builder()
			.localUrl(new URL("http://node-1"))
			.build()
		def group = new PostgresNodeGroup("test-id", 99, [n1])
		when: "we call update() on the node group"
		group.update(connection)
		then: "a statement has been run against the SQL connection"
		true
	}

	def "If an update fails, then throw an exception"() {
		given: "a SQL connection"
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
		when: "we call update() on the node group"
		group.update(connection)
		then: "a Version Changed Exception is thrown"
		thrown(VersionChangedException)
	}

	def "can delete groups"() {
		given: "a SQL connection"
		def connection = Mock(Connection) {
			1* prepareStatement(_) >> Mock(PreparedStatement) {
				1* executeUpdate() >> 1
			}
		}
		and: "a PostgresNodeGroup"
		Node n1 = Node.builder()
			.localUrl(new URL("http://node-1"))
			.build()
		def group = new PostgresNodeGroup("test-id", 99, [n1])
		when: "we call delete() on the node group"
		group.delete(connection)
		then: "a statement has been run against the SQL connection"
		true
	}

	def "If a delete fails, then throw an exception"() {
		given: "a SQL connection"
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
		when: "we call update() on the node group"
		group.delete(connection)
		then: "a Version Changed Exception is thrown"
		thrown(VersionChangedException)
	}
}
