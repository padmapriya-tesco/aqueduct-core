package com.tesco.aqueduct.pipe.codec.http

import com.nixxcode.jvmbrotli.dec.BrotliInputStream
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.pipe.codec.GzipCodec
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpMethod
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.http.simple.SimpleHttpRequest
import io.micronaut.http.simple.SimpleHttpResponse
import io.reactivex.Flowable
import spock.lang.Specification
import spock.lang.Unroll

import java.util.zip.GZIPInputStream

class HttpCodecServerFilterSpec extends Specification {

    def httpCodecServerFilter = new HttpCodecServerFilter(new GzipCodec(), new BrotliCodec())

    @Unroll
    def "http response is encoded as #encoding when content-encoding header is #encoding"() {
        given: "a http request"
        MutableHttpRequest mutableHttpRequest = new SimpleHttpRequest(HttpMethod.GET, "/pipe/0", null)

        and: "having content-encoding header as Gzip"
        mutableHttpRequest.header(HttpHeaders.CONTENT_ENCODING, encoding)

        and: "a server filter chain returning a response"
        MutableHttpResponse httpResponse = new SimpleHttpResponse()
        httpResponse.body(someRichJson())
        def filterChain = Mock(ServerFilterChain)
        filterChain.proceed(mutableHttpRequest) >> Flowable.just(httpResponse)

        when:
        MutableHttpResponse<byte[]> encodedResponse =
            Flowable.fromPublisher(httpCodecServerFilter.doFilter(mutableHttpRequest, filterChain)).blockingFirst()

        then:
        decode(encoding, encodedResponse.body() as byte[]) == someRichJson()

        where:
        encoding | _
        "gzip"   | _
        "brotli" | _
    }

    def "Un-encoded http response when content encoding header is not present"() {
        given: "a http request with no content encoding header"
        MutableHttpRequest mutableHttpRequest = new SimpleHttpRequest(HttpMethod.GET, "/pipe/0", null)

        and: "a server filter chain returning a response"
        MutableHttpResponse httpResponse = new SimpleHttpResponse()
        httpResponse.body(someRichJson())
        def filterChain = Mock(ServerFilterChain)
        filterChain.proceed(mutableHttpRequest) >> Flowable.just(httpResponse)

        when:
        MutableHttpResponse<String> unEncodedResponse =
            Flowable.fromPublisher(httpCodecServerFilter.doFilter(mutableHttpRequest, filterChain)).blockingFirst()

        then:
        unEncodedResponse.body() == someRichJson()
    }

    def "Return body as it is, if not present in the response"() {
        given: "a http request with no content encoding header"
        MutableHttpRequest mutableHttpRequest = new SimpleHttpRequest(HttpMethod.GET, "/pipe/0", null)

        and: "a server filter chain with no response"
        MutableHttpResponse httpResponse = new SimpleHttpResponse()
        def filterChain = Mock(ServerFilterChain)
        filterChain.proceed(mutableHttpRequest) >> Flowable.just(httpResponse)

        when:
        MutableHttpResponse<String> noResponse =
            Flowable.fromPublisher(httpCodecServerFilter.doFilter(mutableHttpRequest, filterChain)).blockingFirst()

        then:
        noResponse.body() == null
    }

    String decode(String encoding, byte[] input) {
        if (encoding.equals("gzip")) {
            new GZIPInputStream(new ByteArrayInputStream(input)).text
        } else if (encoding.equals("brotli")) {
            new BrotliInputStream(new ByteArrayInputStream(input)).text
        } else {
            null
        }
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
