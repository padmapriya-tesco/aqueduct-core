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

    def "get node by host"() {
        given: "A node with a host"
        def url = new URL("http://node-1-url")
        def node = Node.builder()
            .localUrl(url)
            .pipe(["v":"1.0"])
            .build()

        and: "A node with a different host"
        def anotherUrl = new URL("http://node-2-url")
        def anotherNode = Node.builder()
            .localUrl(anotherUrl)
            .pipe(["v":"1.0"])
            .build()

        and: "a Group with these nodes"
        subNodeGroup.add(node)
        subNodeGroup.add(anotherNode)

        when: "fetching a node by host"
        def result = subNodeGroup.getByHost(node.getHost())

        then: "the correct node is returned"
        result == node

        when: "fetching the other node by host"
        result = subNodeGroup.getByHost(anotherNode.getHost())

        then: "the correct node is returned"
        result == anotherNode

        when: "fetching a node with a host that doesn't exist"
        result = subNodeGroup.getByHost("non_existent_id")

        then: "null is returned"
        result == null
    }
}
