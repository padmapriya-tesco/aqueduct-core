package com.tesco.aqueduct.pipe.http.client

import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.registry.client.PipeServiceInstance
import com.tesco.aqueduct.registry.client.SelfRegistrationTask
import com.tesco.aqueduct.registry.client.ServiceList
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import org.junit.Rule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*

class InternalHttpPipeClientIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    InternalHttpPipeClient client

    def setup() {
        context = ApplicationContext
            .build()
            .properties(
                "pipe.http.latest-offset.attempts": 1,
                "pipe.http.latest-offset.delay": "1s",
                "pipe.http.client.url": wireMockRule.baseUrl(),
                "registry.http.client.url": wireMockRule.baseUrl() + "/v2",
                "micronaut.caches.health-check.maximum-size": 20,
                "micronaut.caches.health-check.expire-after-write": "5s"
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .registerSingleton(Mock(TokenProvider))
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl())),
                File.createTempFile("provider", "properties")
            ))
            .start()
        client = context.getBean(InternalHttpPipeClient)
    }

    def "Client is calling correct link with proper parameters with Brotli encoding"() {
        given:
        def responseString = """[
            {
                "type": "type1",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""
        def brotliCodec = new BrotliCodec()
        def encodedBytes = brotliCodec.encode(responseString.bytes)

        and:
        stubFor(
            get(urlEqualTo("/pipe/0?type=type1&location=location1"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('brotli'))
            .willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withHeader("Content-Encoding", "brotli")
                .withStatus(200)
                .withBody(encodedBytes)
            )
        )
        when:
        byte[] responseBytes = client.httpRead(["type1"], 0, "location1").body()

        then:
        responseBytes == encodedBytes
    }

    @Unroll
    def "Client is calling correct link with proper parameters"() {
        given:
        def responseString = """[
            {
                "type": "type1",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""
        def brotliCodec = new BrotliCodec()
        def encodedBytes = brotliCodec.encode(responseString.bytes)

        and:
        stubFor(
            get(urlEqualTo("/pipe/$offset?type=$type&location=$location"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('brotli'))
                .willReturn(aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withHeader("Content-Encoding", "brotli")
                    .withStatus(200)
                    .withBody(encodedBytes)
                )
        )

        when:
        client.httpRead([type], offset, location).body()

        then:
        verify(
            getRequestedFor(urlEqualTo("/pipe/$offset?type=$type&location=$location"))
                .withHeader('Accept', equalTo('application/json'))
                .withHeader('Accept-Encoding', equalTo('brotli'))
        )

        where:
        type    | location    | ct             | offset
        "type1" | "location1" | "contentType1" | 123
        "type2" | "location2" | "contentType2" | 123
        "type1" | "location1" | "contentType3" | 111
        "type2" | "location2" | "contentType4" | 123
    }
}
