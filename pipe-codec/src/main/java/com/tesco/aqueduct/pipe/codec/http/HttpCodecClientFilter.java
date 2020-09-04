package com.tesco.aqueduct.pipe.codec.http;

import com.tesco.aqueduct.pipe.codec.BrotliCodec;
import com.tesco.aqueduct.pipe.codec.CodecType;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.netty.FullNettyClientHttpResponse;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import io.netty.buffer.Unpooled;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.inject.Inject;

@Filter(serviceId = "pipe")
@Requires(property = "pipe.http.client.url")
public class HttpCodecClientFilter implements HttpClientFilter {

    private final BrotliCodec brotliCodec;

    @Inject
    public HttpCodecClientFilter(BrotliCodec brotliCodec) {
        this.brotliCodec = brotliCodec;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(MutableHttpRequest<?> request, ClientFilterChain chain) {
        return Flowable.fromPublisher(chain.proceed(request))
            .map(httpResponse -> {
                if (isResponseBrotliEncoded(httpResponse) && isResponseOfTypeByteArray(httpResponse)) {
                    byte[] encodedResponse = (byte[]) httpResponse.body();
                    final byte[] decodedResponse = brotliCodec.decode(encodedResponse);

                    if (decodedResponse != null && httpResponse instanceof FullNettyClientHttpResponse) {
                        return (HttpResponse<?>)((FullNettyClientHttpResponse) httpResponse)
                            .toFullHttpResponse()
                            .replace(Unpooled.copiedBuffer(decodedResponse));
                    }
                }
                return httpResponse;
            });
    }

    private boolean isResponseOfTypeByteArray(HttpResponse<?> httpResponse) {
        return httpResponse.getBody().isPresent() && httpResponse.body() instanceof byte[];
    }

    private boolean isResponseBrotliEncoded(HttpResponse<?> httpResponse) {
        final String contentEncoding = httpResponse.getHeaders().get(HttpHeaders.CONTENT_ENCODING);
        return contentEncoding != null && contentEncoding.contains(CodecType.BROTLI.name().toLowerCase());
    }
}
