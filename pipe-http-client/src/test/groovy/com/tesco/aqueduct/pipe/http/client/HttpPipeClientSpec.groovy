package com.tesco.aqueduct.pipe.http.client

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.registry.PipeLoadBalancer
import io.micronaut.http.HttpResponse
import spock.lang.Specification

class HttpPipeClientSpec extends Specification {

    InternalHttpPipeClient internalClient = Mock()
    HttpPipeClient client = new HttpPipeClient(internalClient, Mock(PipeLoadBalancer))

    def "a read from the implemented interface method returns a result with the retry after and messages"() {
        given: "call returns a http response with retry after header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [ Mock(Message) ]
        response.header("Retry-After") >> retry
        internalClient.httpRead(_ as Map,_ as Long) >> response

        when: "we call read and get defined response back"
        def results = client.read([:], 0)

        then: "we parse the retry after if its correct or return 0 otherwise"
        results.messages.size() == 1
        results.retryAfterSeconds == result

        where:
        retry | result
        ""    | 0
        null  | 0
        "5"   | 5
        "-5"  | 0
        "foo" | 0
    }

    def "allows to get latest offset" () {
        given:
        internalClient.getLatestOffsetMatching(tags) >> offset

        when:
        def response = client.getLatestOffsetMatching(tags)

        then:
        response == offset

        where:
        tags       | offset
        [x: ["y"]] | 7
        [x: ["y"]] | 8
        [:]        | 9
    }
}
