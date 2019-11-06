package com.tesco.aqueduct.registry.postgres

import com.tesco.aqueduct.registry.model.Node
import spock.lang.Specification

import javax.sql.DataSource
import java.time.Duration

class PostgreSQLNodeRegistrySpec extends Specification {
	def "New node can register"() {
		given: "a mock node group factory"
			def cloudUrl = new URL("http://cloud.url")
			def mockNodeGroup = Mock(PostgresNodeGroup)
			1* mockNodeGroup.getById(_) >> null
			1* mockNodeGroup.add(_, _) >> Node.builder().requestedToFollow([cloudUrl]).build()
			def mockNodeGroupFactory = Mock(PostgresNodeGroupStorage)
			1* mockNodeGroupFactory.getNodeGroup(_, _) >> mockNodeGroup
		and: "a node registry"
			def dataSourceMock = Mock(DataSource)
			def offlineDelta = Duration.ofMinutes(5)
			def registry = new PostgreSQLNodeRegistry(dataSourceMock, cloudUrl, offlineDelta, mockNodeGroupFactory)
		and: "a node to register"
			def testNode = Mock(Node)
		when: "registering the node"
			def result = registry.register(testNode)
		then: "we cloud url is returned"
			result == [cloudUrl]
	}
}
