package com.tesco.aqueduct.registry.model

import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class NodeGroupFactorySpec extends Specification {
	def "can fetch a NodeGroup"() {
		String statement
		given: "a SQL connection"
		def connection = Mock(Connection) {
			1* prepareStatement(_) >> {
				statement = it
				return Mock(PreparedStatement) {
					1 * executeQuery() >> Mock(ResultSet) {
						1* next() >> true
						1* getString("entry") >>
							"[" +
								"{" +
									"\"localUrl\":\"http://node-1\"," +
									"\"offset\":\"0\"," +
									"\"providerLastAckOffset\":\"0\"," +
									"\"id\":\"http://node-1\"" +
								"}" +
							"]"
						1* getInt("version") >> 99
						1* getString("group_id") >> "test-group-id-from-db"
					}
				}
			}
		}
		when: "I ask the factory to create a NodeGroup"
		def result = NodeGroupFactory.getNodeGroup(connection, "test-group-id")
		then: "then a select statement is run"
		statement.contains("SELECT")
		and: "The NodeGroup is correctly created"
		result.nodes[0].localUrl == new URL("http://node-1")
		result.version == 99
		result.groupId == "test-group-id-from-db"
	}
}
