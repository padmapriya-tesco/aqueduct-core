package com.tesco.aqueduct.pipe.codec.http


import com.tesco.aqueduct.pipe.codec.BrotliCodec
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpMethod
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.ClientFilterChain
import io.micronaut.http.simple.SimpleHttpRequest
import io.micronaut.http.simple.SimpleHttpResponse
import io.reactivex.Flowable
import spock.lang.Specification

class HttpCodecClientFilterSpec extends Specification {

    def "response bytes are decoded if response accept-encoding is Brotli"() {
        given:
        def brotliCodec = new BrotliCodec(4)
        HttpCodecClientFilter clientFilter = new HttpCodecClientFilter(brotliCodec)

        and: "a brotli compressed response"
        def responseJson = someRichJson()
        def encodedResponse = brotliCodec.encode(responseJson.bytes)

        and: "http request"
        MutableHttpRequest httpRequest = new SimpleHttpRequest(HttpMethod.GET, "/pipe/0?location=lid", null)
        httpRequest.header(HttpHeaders.ACCEPT_ENCODING, "brotli")

        and: "http response"
        MutableHttpResponse httpResponse = new SimpleHttpResponse()
        httpResponse.header(HttpHeaders.CONTENT_ENCODING, "brotli")
        httpResponse.body(encodedResponse)

        and: "mocked filter chain"
        ClientFilterChain filterChain = Mock()
        filterChain.proceed(httpRequest) >> Flowable.just(httpResponse)

        when:
        def response = Flowable.fromPublisher(clientFilter.doFilter(httpRequest, filterChain)).blockingFirst()

        then:
        response.body() instanceof String

        response.body() as String == responseJson
    }

    def "response bytes are untouched when content-encoding is not brotli"() {
        given:
        def brotliCodec = new BrotliCodec(4)
        HttpCodecClientFilter clientFilter = new HttpCodecClientFilter(brotliCodec)

        and: "a non brotli encoded response"
        def responseJson = someRichJson()

        and: "http request"
        MutableHttpRequest httpRequest = new SimpleHttpRequest(HttpMethod.GET, "/pipe/0?location=lid", null)
        httpRequest.header(HttpHeaders.ACCEPT_ENCODING, "gzip")

        and: "http response"
        MutableHttpResponse httpResponse = new SimpleHttpResponse()
        httpResponse.header(HttpHeaders.CONTENT_ENCODING, "gzip")
        httpResponse.body(responseJson.bytes)

        and: "mocked filter chain"
        ClientFilterChain filterChain = Mock()
        filterChain.proceed(httpRequest) >> Flowable.just(httpResponse)

        when:
        def response = Flowable.fromPublisher(clientFilter.doFilter(httpRequest, filterChain)).blockingFirst()

        then:
        response.body() instanceof byte[]

        response.body() as String == responseJson
    }

    String someRichJson() {
        """
            [
              {
                "type": "type1",
                "key": "A",
                "contentType": "content-type",
                "offset": "1",
                "created": "2000-12-01T10:00:00Z",
                "data": "{\"type\":\"someType\",\"id\":\"someId\"}"
              },
              {
                "type": "type2",
                "key": "D",
                "contentType": "content-type",
                "offset": "4",
                "created": "2000-12-01T10:00:00Z",
                "data": "{\"id\":\"someId\",\"arraykey\":[\"2231bb94-05c9-4914-a37e-55ce1907fd02\"],\"number\":\"8899\"}"
              },
              {
                "type": "type2",
                "key": "D",
                "contentType": "content-type",
                "offset": "5",
                "created": "2000-12-01T10:00:00Z",
                "data": "łąś»«·§≠²³€²≠³½ßśćąż"
              }
            ]
        """
    }

}
