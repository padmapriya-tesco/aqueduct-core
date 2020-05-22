package com.tesco.aqueduct.registry.model

import spock.lang.Specification

class SubNodeGroupSpec extends Specification {

    SubNodeGroup subNodeGroup;
    private final static String PIPE_VERSION = "1.0";

    def setup () {
        subNodeGroup = new SubNodeGroup(PIPE_VERSION)
    }

    def "add node"() {
        given: "A node with a local url"
        def nodeUrl = new URL("http://node-1-url")
        def node = Node.builder()
            .localUrl(nodeUrl)
            .pipe(["v":"1.0"])
            .build()

        and: "A node with a different id"
        def anotherUrl = new URL("http://node-2-url")
        def anotherNode = Node.builder()
            .localUrl(anotherUrl)
            .pipe(["v":"1.0"])
            .build()

        when: "nodes are added"
        def cloudUrl = new URL("http://some-cloud-url")
        subNodeGroup.add(node, cloudUrl)
        subNodeGroup.add(anotherNode, cloudUrl)

        then: "nodes have been added"
        subNodeGroup.nodes.size() == 2
        subNodeGroup.nodes.get(0).localUrl == nodeUrl
        subNodeGroup.nodes.get(1).localUrl == anotherUrl

        and: "the first node follows the cloud"
        subNodeGroup.nodes.get(0).requestedToFollow == [cloudUrl]

        and: "the second node follows the first one"
        subNodeGroup.nodes.get(1).requestedToFollow == [nodeUrl, cloudUrl]
    }

    def "get node by id"() {
        given: "A node with an id"
        def url = new URL("http://node-1-url")
        def node = Node.builder()
            .localUrl(url)
            .pipe(["v":"1.0"])
            .build()

        and: "A node with a different id"
        def anotherUrl = new URL("http://node-2-url")
        def anotherNode = Node.builder()
            .localUrl(anotherUrl)
            .pipe(["v":"1.0"])
            .build()

        and: "a Group with these nodes"
        subNodeGroup.add(node)
        subNodeGroup.add(anotherNode)

        when: "fetching a node by id"
        def result = subNodeGroup.getById(node.getId())

        then: "the correct node is returned"
        result == node

        when: "fetching the other node by id"
        result = subNodeGroup.getById(anotherNode.getId())

        then: "the correct node is returned"
        result == anotherNode

        when: "fetching a node with an id that doesn't exist"
        result = subNodeGroup.getById("non_existent_id")

        then: "null is returned"
        result == null
    }
}
