package com.tesco.aqueduct.pipe.codec.netty.handler;

import com.tesco.aqueduct.pipe.codec.BrotliCodec;
import com.tesco.aqueduct.pipe.codec.GzipCodec;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentEncoder;
import io.netty.handler.codec.http.HttpResponse;

public class HttpCodecHandler extends HttpContentEncoder {

    private final GzipCodec gzipCodec;
    private final BrotliCodec brotliCodec;
    private ChannelHandlerContext ctx;

    public HttpCodecHandler(final GzipCodec gzipCodec, final BrotliCodec brotliCodec) {
        this.gzipCodec = gzipCodec;
        this.brotliCodec = brotliCodec;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    protected Result beginEncode(HttpResponse httpResponse, String acceptEncoding) throws Exception {

        if (acceptEncoding.contains("brotli")) {

            return new Result("brotli",
                new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), gzipCodec));
        } else {
            return new Result("gzip",
                new EmbeddedChannel(ctx.channel().id(), ctx.channel().metadata().hasDisconnect(),
                    ctx.channel().config(), brotliCodec));
        }
    }
}
