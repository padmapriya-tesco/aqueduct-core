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
        ServiceList serviceList = new ServiceList(config, URL_1)
        def list = [URL_1, URL_2, URL_3]

        when: "service list is updated"
        serviceList.update(list)

        then: "list returned matches updated list"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == list
    }

    def "when services are updated, obsolete services are removed"() {
        given: "a service list"
        ServiceList serviceList = new ServiceList(config, URL_1)

        and: "the service list has been updated before"
        serviceList.update([URL_1, URL_2])

        when: "service list is updated with a new list"
        def list = [URL_2, URL_3]
        serviceList.update(list)

        then: "list returned matches updated list"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == list
    }

    def "when services are updated, previous services keep their status"() {
        given: "a service list"
        ServiceList serviceList = new ServiceList(config, URL_1)

        and: "the service list has been updated before"
        serviceList.update([URL_1, URL_2])

        and: "the service statuses have been set"
        serviceList.services[0].isUp(true)
        serviceList.services[1].isUp(false)

        when: "service list is updated with a new list"
        def list = [URL_1, URL_2, URL_3]
        serviceList.update(list)

        then: "list returned matches updated list"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == list

        and: "service statuses have not been altered"
        serviceList.stream().map({p -> p.isUp()}).collect() == [true, false, true]
    }

    def "service list always contains at least the cloud url"() {
        given: "a service list"
        ServiceList serviceList = new ServiceList(config, URL_1)

        when: "the service list has not been updated yet"

        then: "the service list has the cloud_url"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == [URL_1]

        when: "the service list is updated with a null list"
        serviceList.update(null)

        then: "the service list has the cloud url"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == [URL_1]

        when: "the service list is updated with an empty list"
        serviceList.update([])

        then: "the service list has the cloud url"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == [URL_1]
    }

    def "service list does not contain cloud url if updated with a list without it"() {
        given: "a service list"
        ServiceList serviceList = new ServiceList(config, URL_1)

        when: "the service list is updated with a list without the cloud url"
        serviceList.update([URL_2, URL_3])

        then: "the service list does not have the cloud url"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == [URL_2, URL_3]

        when: "the service list is updated with an empty list"
        serviceList.update([])

        then: "the service list has the cloud url"
        serviceList.stream().map({ p -> p.getUrl()}).collect() == [URL_1]
    }

    //TODO: once we have persistence etc., add a test to assert that at any state something is returned when getServices is called
}
