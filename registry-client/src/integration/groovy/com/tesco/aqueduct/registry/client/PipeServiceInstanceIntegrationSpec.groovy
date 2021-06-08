package com.tesco.aqueduct.registry.client

import com.github.tomakehurst.wiremock.junit.WireMockRule
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import org.junit.Rule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static com.github.tomakehurst.wiremock.client.WireMock.*

class PipeServiceInstanceIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    def "sets flag isUp to true when a valid http response is returned"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withBody("a response")
                )
        )

        when:
        pipeServiceInstance.checkState().blockingGet()

        then:
        pipeServiceInstance.isUp()
    }

    def "checkState retries once if request fails on first attempt"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(500)
                    .withBody("a response")
                )
        )

        when:
        pipeServiceInstance.checkState().blockingGet()

        then:
        pipeServiceInstance.isUp()
    }
}
