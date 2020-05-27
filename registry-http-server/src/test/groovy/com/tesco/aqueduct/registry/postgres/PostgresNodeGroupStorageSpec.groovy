package com.tesco.aqueduct.registry.postgres


import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class PostgresNodeGroupStorageSpec extends Specification {
	def "can fetch a NodeGroup"() {
		String statement

		given: "a SQL connection"
		def connection = Mock(Connection) {
			1 * prepareStatement("BEGIN WORK;") >> {
				return Mock(PreparedStatement) {
					1 * execute() >> true
				}
			}

			1 * prepareStatement(_) >> {
				statement = it
				return Mock(PreparedStatement) {
					1 * executeQuery() >> Mock(ResultSet) {
						1 * next() >> true
						1 * getString("entry") >>
						"""		
							[ 
								{ 
									"localUrl":"http://node-1",
									"offset":"0",
									"status":"OFFLINE",
									"providerLastAckOffset":"0",
									"id":"http://node-1",
									"pipe":{"v":"1.0"}
								}
							]
						"""
						1 * getInt("version") >> 99
						1 * getString("group_id") >> "test-group-id-from-db"
					}
				}
			}
		}

		and: "a PostgresNodeGroupStorage"
		def PostgresNodeGroupStorage = new PostgresNodeGroupStorage()

		when: "I ask the PostgresNodeGroupStorage to create a NodeGroup"
		def result = PostgresNodeGroupStorage.getNodeGroup(connection, "test-group-id")

		then: "then a select statement is run"
		statement.contains("SELECT")

		and: "The NodeGroup is correctly created"
		result.nodes[0].localUrl == new URL("http://node-1")
		result.version == 99
		result.groupId == "test-group-id-from-db"
	}

	def "can fetch a list of NodeGroups"() {
		String statement

		given: "a SQL connection"
		def connection = Mock(Connection) {
			1 * prepareStatement(_) >> {
				statement = it
				return Mock(PreparedStatement) {
					1 * executeQuery() >> Mock(ResultSet) {
						1 * next() >> true
						1 * getString("entry") >>
						"""		
							[ 
								{ 
									"localUrl":"http://node-1",
									"offset":"0",
									"status":"OFFLINE",
									"providerLastAckOffset":"0",
									"id":"http://node-1",
									"pipe":{"v":"1.0"}
								}
							]
						"""
						1 * getInt("version") >> 99
						1 * getString("group_id") >> "test-group-id-from-db"
					}
				}
			}
		}

		and: "a PostgresNodeGroupStorage"
		def PostgresNodeGroupStorage = new PostgresNodeGroupStorage()

		when: "I ask the PostgresNodeGroupStorage to get a list of NodeGroups"
		def result = PostgresNodeGroupStorage.readNodeGroups(connection, ["test-group-id"])

		then: "then a select statement is run"
		statement.contains("SELECT")

		and: "The NodeGroups are correctly returned"
		result[0].nodes[0].localUrl == new URL("http://node-1")
		result[0].version == 99
		result[0].groupId == "test-group-id-from-db"
	}

	def "can fetch all NodeGroups"() {
		String statement

		given: "a SQL connection"
		def connection = Mock(Connection) {
			1 * prepareStatement(_) >> {
				statement = it
				return Mock(PreparedStatement) {
					1 * executeQuery() >> Mock(ResultSet) {
						2 * next() >> true >> false
						1 * getString("entry") >>
						"""		
							[ 
								{ 
									"localUrl":"http://node-1",
									"offset":"0",
									"status":"OFFLINE",
									"providerLastAckOffset":"0",
									"id":"http://node-1",
									"pipe":{"v":"1.0"}
								}
							]
						"""
						1 * getInt("version") >> 99
						1 * getString("group_id") >> "test-group-id-from-db"
					}
				}
			}
		}

		and: "a PostgresNodeGroupStorage"
		def PostgresNodeGroupStorage = new PostgresNodeGroupStorage()

		when: "I ask the PostgresNodeGroupStorage to get a list of NodeGroups, providing no ids"
		def result = PostgresNodeGroupStorage.readNodeGroups(connection, [])

		then: "then a select statement is run"
		statement.contains("SELECT")

		and: "The NodeGroups are correctly returned"
		result[0].nodes[0].localUrl == new URL("http://node-1")
		result[0].version == 99
		result[0].groupId == "test-group-id-from-db"
	}
}
