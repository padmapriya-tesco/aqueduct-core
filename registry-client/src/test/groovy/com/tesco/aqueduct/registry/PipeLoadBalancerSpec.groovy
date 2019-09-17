package com.tesco.aqueduct.registry

import com.stehno.ersatz.ErsatzServer
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.http.uri.UriBuilder
import io.reactivex.Flowable
import spock.lang.Specification

import static io.reactivex.Single.fromPublisher


@Newify(URL)
class PipeLoadBalancerSpec extends Specification {

    final static URL URL_1 = URL("http://a1")
    final static URL URL_2 = URL("http://a2")
    final static URL URL_3 = URL("http://a3")

    PipeLoadBalancer loadBalancer

    def setup() {
        loadBalancer = new PipeLoadBalancer(new DefaultHttpClientConfiguration())
    }

    def "Don't return a service until the registry has been updated"() {
        when: "that there is no service available"
        fromPublisher(loadBalancer.select()).blockingGet()

        then:
        thrown Exception

        when: "the registry is updated and I select the service instance"
        loadBalancer.update([URL_1])
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then:
        serviceInstance.URI == URL_1.toURI()
    }

    def "After updating the registry the first url in the hitlist is selected"() {
        given: "that the registry is updated"
        def tillOneUrl = URL("http://till-1.pipe:8080")
        def urls = [tillOneUrl, URL("http://till-2.pipe:8080")]
        loadBalancer.update(urls)

        when: "I select the service instance"
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then: "the service points to the first url in the registry"
        serviceInstance.URI == tillOneUrl.toURI()
    }

    def "Returns a list of URLs"() {
        def urls = [URL_1, URL_3, URL_3]

        given: "An updated list of urls"
        loadBalancer.update(urls)

        when: "we call get"
        def actualUrls = loadBalancer.getFollowing()

        then: "we are returned with a list of following URLS"
        urls == actualUrls
    }

    def "Returns a list of URLS that are up"() {
        def urls = [URL_1, URL_2, URL_3]
        def expectedUrls = [URL_2, URL_3]

        given: "An updated list of urls"
        loadBalancer.update(urls)

        and: "We record an error"
        loadBalancer.recordError()

        when: "we call get"
        def actualUrls = loadBalancer.getFollowing()

        then: "we are returned with a list of following URLS that are up"
        expectedUrls == actualUrls
    }

    def "Updating the list of urls does not change UP status"() {
        given: "2 service urls and one marked as down"
        loadBalancer.update([URL_1, URL_2])
        loadBalancer.recordError() // will mark URL_1 as down

        when: "we update list of urls again"
        loadBalancer.update([URL_1, URL_2, URL_3])

        then: "the statuses of existing services have not been change"
        loadBalancer.getFollowing() == [URL_2, URL_3] // first service is down hence 2 and 3 is returned
    }

    def "In case of error, pick the next one"() {
        given: "a list of urls"
        loadBalancer.update([URL_1, URL_2, URL_3])

        when: "we got an error from the client"
        loadBalancer.recordError()

        and: "we select anothet service"
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then: "we getting next instance"
        serviceInstance.URI.toString() == "http://a2"
    }

    def "check state respects the base path of all the servers in the list when performing the status check"() {

        given: "a list of paths exist which contains urls with and without a base path"
        def paths = ["/messaging/v1", "", "/"]

        and: "servers that respond a 200 status code for each URL exist"
        def servers = paths.collect { serverWithPipeStatus(it, 200) }

        and: "a load balancer is updated with the list of urls from the mock servers"
        loadBalancer.update(paths.withIndex().collect { element, index -> URL(servers[index].httpUrl + element) })

        when: "checking the status of the urls in the load balancer"
        loadBalancer.checkState()

        then: "the mock servers are called, showing that the check status method respects the base path of the endpoints"
        servers.each { it.verify() }

        cleanup: "the mock servers are cleaned up"
        servers.each { it.close() }
    }

    def "check state checks the status of all servers on the list"() {
        given: "a load balancer with a list of server urls"
        List<ErsatzServer> servers = [serverWithPipeStatus(200), serverWithPipeStatus(200)]
        servers.each { it.start() }

        loadBalancer.update(servers.collect{ URL(it.httpUrl) })

        when: "check state is called"
        loadBalancer.checkState()

        then:"status endpoints are called on each server"
        servers.each { it.verify() }

        cleanup:
        servers.each { it.close() }
    }

    def "RxClient errors are not rethrown"() {
        given: "client throwing errors"
        def serviceInstance = new PathRespectingPipeInstance(new URL("http://not.a.url"), true)

        when: "we check the state"
        loadBalancer.checkState(serviceInstance).blockingAwait()

        then:
        noExceptionThrown()

        and:
        !serviceInstance.isUp()
    }

    ErsatzServer serverWithPipeStatus(int status) {
        ErsatzServer server = new ErsatzServer()
        server.expectations {
            get("/pipe/_status") {
                called(1)

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(status)
                }
            }
        }

        return server
    }

    ErsatzServer serverWithPipeStatus(String path, int status) {
        ErsatzServer server = new ErsatzServer()
        server.expectations {
            String urlAsString = UriBuilder.of(URI.create(path)).path("/pipe/_status").build().toString();
            get(urlAsString) {
                called(1)

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(status)
                }
            }
        }

        return server
    }
}
