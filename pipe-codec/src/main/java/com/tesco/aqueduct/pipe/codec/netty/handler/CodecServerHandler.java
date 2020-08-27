package com.tesco.aqueduct.pipe.codec.netty.handler;

import com.tesco.aqueduct.pipe.codec.BrotliCodec;
import com.tesco.aqueduct.pipe.codec.GzipCodec;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class CodecServerHandler extends ChannelDuplexHandler {

    private final BrotliCodec brotliCodec;
    private final GzipCodec gzipCodec;

    public CodecServerHandler(BrotliCodec brotliCodec, GzipCodec gzipCodec) {
        this.brotliCodec = brotliCodec;
        this.gzipCodec = gzipCodec;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // TODO -
        //  Decide whether to code brotli or gzip using accept-encoding header in request (Need to find how to get hold off HttpRequest object here),
        //  Get body from msg (msg is of type NettyHttpResponse ) reference: https://github.com/zalando/logbook/blob/master/logbook-netty/src/main/java/org/zalando/logbook/netty/LogbookServerHandler.java
        super.write(ctx, msg, promise);
    }
}
