package com.tesco.aqueduct.registry.model

import spock.lang.Specification

class NodeSpec extends Specification {
    def "Node has id"() {
        given: "A node with a URL and no group"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .build()
        when: "Getting the node id"
        def result = testNode.getId()
        then: "the result is the localUrl"
        result == "http://test-url"
    }

    def "Node has id containing group"() {
        given: "A node with a URL and group"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .group("group-name")
            .build()
        when: "Getting the node id"
        def result = testNode.getId()
        then: "the result is a combination of group name and url"
        result == "group-name|http://test-url"
    }

    def "Can get Node host"() {
        given: "A node with a URL"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .build()
        when: "Getting the node host"
        def result = testNode.getHost()
        then: "the result is the host from the URL"
        result == "test-url"
    }
}
