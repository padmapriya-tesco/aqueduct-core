package com.tesco.aqueduct.registry

import io.micronaut.http.client.HttpClientConfiguration
import spock.lang.Specification
import spock.lang.Unroll

@Newify(URL)
class PathRespectingPipeInstanceSpec extends Specification {

    @Unroll
    def "For path base url #baseUrl resolved path should be #expectedPath"() {
        given: "A url with a base path"
        def url = URL(baseUrl)
        def uri = new URI("/pipe/0")
        def serviceInstance = new PathRespectingPipeInstance(Mock(HttpClientConfiguration), url, true)

        when: "resolving a relative uri"
        def response = serviceInstance.resolve(uri)

        then: "the path is returned with the base path included"
        response.path == expectedPath

        where:
        baseUrl               | expectedPath
        "http://foo.bar/bar"  | "/bar/pipe/0"
        "http://foo.bar/bar/" | "/bar/pipe/0"
        "http://foo.bar/"     | "/pipe/0"
        "http://foo.bar"      | "/pipe/0"
    }
}
