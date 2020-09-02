package com.tesco.aqueduct.pipe.codec.netty.handler

import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.pipe.codec.GzipCodec
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPromise
import io.netty.handler.codec.http.*
import spock.lang.Specification

class CodecServerHandlerSpec extends Specification {

    def "Response is gzip encoded when accept encoding header says gzip"() {
        given:
        def brotliCodec = Mock(BrotliCodec)
        def gzipCodec = Mock(GzipCodec)

        and:
        CodecServerHandler codecServerHandler = new CodecServerHandler(brotliCodec, gzipCodec)

        and: "Mocked Http request"
        HttpRequest httpRequest =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/pipe/0?location=l1")
        httpRequest.headers().set(io.micronaut.http.HttpHeaders.ACCEPT_ENCODING, "gzip,deflate")

        and: "Mocked Http response and context"
        HttpResponse httpResponse =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK, Unpooled.copiedBuffer("some body".bytes))
        def responseContext = Mock(ChannelHandlerContext)

        when: "read and write using codec handler"
        codecServerHandler.channelRead(Mock(ChannelHandlerContext), httpRequest)
        codecServerHandler.write(responseContext, httpResponse, Mock(ChannelPromise))

        then:
        1 * gzipCodec.encode("some body".bytes) >> "some encoded bytes".bytes

        1 * responseContext.write(_, _) >> { arg ->
            assert arg[0] instanceof DefaultFullHttpResponse
            def response = (DefaultFullHttpResponse) arg[0]
            assert response.headers().get(io.micronaut.http.HttpHeaders.CONTENT_ENCODING) == "gzip,deflate"
            assert response.content().array() == "some encoded bytes".bytes

        }
    }
}
