package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.api.Reader
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.pipe.codec.Codec
import io.micronaut.http.MutableHttpRequest
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MicronautTest
import io.micronaut.test.annotation.MockBean
import spock.lang.Specification

import javax.inject.Inject
import javax.inject.Named

@MicronautTest
class PipeReadControllerSpec extends Specification {

    PipeReadController controller = new PipeReadController()

    @Inject @Named("local")
    Reader reader

    @Inject
    Codec brotliCodec

    @Inject
    LocationResolver locationResolver

    @Inject
    EmbeddedServer server

    void setup() {
        locationResolver.resolve(_) >> []
        reader.read([], 0L, []) >> []
    }

    def "messages should be encoded if Accept-Content header set to #codec"() {
        given: 'a read request'
        def request = Mock(MutableHttpRequest)
        Map<String, String> headers = new HashMap<>()
        headers.put("Accept-Content", "br")
        request.headers(headers)

        request.getRemoteAddress() >> new InetSocketAddress("hostname", 8080)

        when: "we read from the pipe"
        controller.readMessages(0L, request, Collections.emptyList(), "some-location")

        then: "the response is encoded"
        1 * brotliCodec.encode(_)
    }

    @MockBean(Reader)
    @Named("local")
    Reader reader() {
        Mock(Reader)
    }

    @MockBean(Codec)
    Codec brotliCodec() {
        Mock(Codec)
    }

    @MockBean(LocationResolver)
    LocationResolver locationResolver() {
        Mock(LocationResolver)
    }
}
