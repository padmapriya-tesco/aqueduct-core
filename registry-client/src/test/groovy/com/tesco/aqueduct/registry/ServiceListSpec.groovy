package com.tesco.aqueduct.registry

import io.micronaut.http.client.DefaultHttpClientConfiguration
import spock.lang.Specification

@Newify(URL)
class ServiceListSpec extends Specification {

    final static URL URL_1 = URL("http://a1")
    final static URL URL_2 = URL("http://a2")
    final static URL URL_3 = URL("http://a3")

    def config = new DefaultHttpClientConfiguration()

    def "services that are updated are returned in the getServices"() {
        given: "a service list"
        ServiceList serviceList = new ServiceList(config)
        def list = [URL_1, URL_2, URL_3]

        when: "service list is updated"
        serviceList.update(list)

        then: "list returned matches updated list"
        serviceList.getServices().stream().map({ p -> p.getUrl()}).collect() == list
    }

    //TODO: once we have persistence etc., add a test to assert that at any state something is returned when getServices is called
}
