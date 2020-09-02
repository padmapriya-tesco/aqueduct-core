package com.tesco.aqueduct.pipe.codec.netty.handler;

import com.tesco.aqueduct.pipe.codec.BrotliCodec;
import com.tesco.aqueduct.pipe.codec.GzipCodec;
import io.micronaut.http.HttpHeaders;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class CodecServerHandler extends ChannelDuplexHandler {

    private final BrotliCodec brotliCodec;
    private final GzipCodec gzipCodec;
    private String acceptEncoding;

    public CodecServerHandler(BrotliCodec brotliCodec, GzipCodec gzipCodec) {
        this.brotliCodec = brotliCodec;
        this.gzipCodec = gzipCodec;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            this.acceptEncoding = ((HttpRequest) msg).headers().get(HttpHeaders.ACCEPT_ENCODING);
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse httpResponse = (FullHttpResponse) msg;

            if (acceptEncoding.contains("gzip")) {
                final byte[] encoded = gzipCodec.encode(httpResponse.content().array());
                final FullHttpResponse newHttpResponse = httpResponse.replace(Unpooled.copiedBuffer(encoded));
//                newHttpResponse.headers().set(HttpHeaders.CONTENT_ENCODING, "gzip");
                super.write(ctx, newHttpResponse, promise);
                return;
            }
        }
        super.write(ctx, msg, promise);
    }
}
