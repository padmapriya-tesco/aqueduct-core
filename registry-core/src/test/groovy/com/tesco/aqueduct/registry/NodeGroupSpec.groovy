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

		and: "a Group with only this node"
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
}
