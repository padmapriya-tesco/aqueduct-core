package com.tesco.aqueduct.pipe.codec.http;

import com.tesco.aqueduct.pipe.codec.BrotliCodec;
import com.tesco.aqueduct.pipe.codec.CodecType;
import com.tesco.aqueduct.pipe.codec.GzipCodec;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.filter.ServerFilterChain;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

//@Filter("/pipe/**")
public class HttpCodecServerFilter  {

/*
    private final GzipCodec gzipCodec;
    private final BrotliCodec brotliCodec;

    @Inject
    public HttpCodecServerFilter(GzipCodec gzipCodec, BrotliCodec brotliCodec) {
        this.gzipCodec = gzipCodec;
        this.brotliCodec = brotliCodec;
    }
*/

//    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        // TODO if payload is small do not encode
        return Flowable.fromPublisher(chain.proceed(request))
            .map(response -> {
                final String contentEncoding = request.getHeaders().get(HttpHeaders.CONTENT_ENCODING);
                if (contentEncoding != null
                    && response.getBody().isPresent()
                    && response.body() instanceof String
                ) {
                    byte[] encodedResponse = null;

                    if (contentEncoding.contains(CodecType.GZIP.name().toLowerCase())) {
                        encodedResponse = new GzipCodec().encode(((String) response.body()).getBytes());

                    } else if (contentEncoding.contains(CodecType.BROTLI.name().toLowerCase())) {
                        encodedResponse = new BrotliCodec().encode(((String) response.body()).getBytes());
                    }
                    if (encodedResponse != null) {
                        ((MutableHttpResponse) response).body(encodedResponse);
                    }
                }
                return response;
            });
    }
}
