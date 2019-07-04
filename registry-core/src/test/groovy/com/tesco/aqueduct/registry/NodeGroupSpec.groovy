package com.tesco.aqueduct.registry

import com.tesco.aqueduct.registry.model.Node
import spock.lang.Specification

class NodeGroupSpec extends Specification {
	def "Group has node"() {
		given: "A Group with Nodes"
		def group = new NodeGroup([Mock(Node)], 1)
		when: "checking the group has nodes"
		def result = group.isEmpty()
		then: "the result is false"
		!result
	}

	def "Group does not have nodes"() {
		given: "A Group with no Nodes"
		def group = new NodeGroup([], 1)
		when: "checking the group has nodes"
		def result = group.isEmpty()
		then: "the result is true"
		result
	}

	def "A node can be removed from a node group given an id"() {
		given: "A node with an id"
		def node = Mock Node
		node.id >> "test_node_id"

		and: "A node with a different id"
		def anotherNode = Mock Node
		anotherNode.id >> "another_node_id"

		and: "a Group with these nodes"
		def group = new NodeGroup([node, anotherNode], 1)

		when: "Removing the node from the group using the id"
		def result = group.removeById("test_node_id")

		then: "the result is true (the node was found and deleted)"
		result

		and: "The group no longer contains the removed node"
		group.nodes == [anotherNode]
	}

	def "a node can be added to the group"() {
		given: "an empty node group"
		def group = new NodeGroup([], 1)
		when: "a new node is added"
		group.add(Mock(Node))
		then: "the node group is no longer empty"
		!group.isEmpty()
	}

	def "a node can be fetched from the group by index"() {
		given: "A node with an id"
		def node = Mock Node
		node.id >> "test_node_id"

		and: "A node with a different id"
		def anotherNode = Mock Node
		anotherNode.id >> "another_node_id"

		and: "a Group with these nodes"
		def group = new NodeGroup([node, anotherNode], 1)

		when: "fetching the node at index 0"
		def result = group.get(0)

		then: "the correct node is returned"
		result == node

		when: "fetching the node at index 1"
		result = group.get(1)

		then: "the correct node is returned"
		result == anotherNode
	}

	def "Fetch URLs for all nodes"() {
		given: "A node with a local url"
		def nodeUrl = new URL("http://test_node_1")
		def node = Mock Node
		node.localUrl >> nodeUrl

		and: "A node with a different id"
		def anotherNodeUrl = new URL("http://test_node_2")
		def anotherNode = Mock Node
		anotherNode.localUrl >> anotherNodeUrl

		and: "a Group with these nodes"
		def group = new NodeGroup([node, anotherNode], 1)

		when: "all Node urls have been fetched"
		def result = group.getNodeUrls()

		then: "Both URLs have been returned"
		result == [nodeUrl, anotherNodeUrl]
	}

	def "get node by id"() {
		given: "A node with an id"
		def node = Mock Node
		node.id >> "test_node_id"

		and: "A node with a different id"
		def anotherNode = Mock Node
		anotherNode.id >> "another_node_id"

		and: "a Group with these nodes"
		def group = new NodeGroup([node, anotherNode], 1)

		when: "fetching a node by id"
		def result = group.getById("test_node_id")

		then: "the correct node is returned"
		result == node

		when: "fetching the other node by id"
		result = group.getById("another_node_id")

		then: "the correct node is returned"
		result == anotherNode

		when: "fetching a node with an id that doesn't exist"
		result = group.getById("non_existent_id")

		then: "null is returned"
		result == null
	}

	def "A node can be updated in the Group"() {
		given: "A node with a local url"
		def nodeUrl = new URL("http://test_node_1")
		def node = Mock Node
		node.id >> "test_node_id"
		node.localUrl >> nodeUrl

		and: "A node with a different id"
		def anotherNode = Mock Node
		anotherNode.id >> "another_node_id"

		and: "a Group with these nodes"
		def group = new NodeGroup([node, anotherNode], 1)

		when: "An updated node is provided to the group"
		def updatedNode = Mock(Node)
		updatedNode.id >> "test_node_id"
		group.updateNode(updatedNode)

		then: "The group contains the updated node"
		group.nodes == [updatedNode, anotherNode]

		when: "Another updated node is provided to the group"
		def anotherUpdatedNode = Mock(Node)
		anotherUpdatedNode.id >> "another_node_id"
		group.updateNode(anotherUpdatedNode)

		then: "The group contains the updated nodes"
		group.nodes == [updatedNode, anotherUpdatedNode]

	}
}
