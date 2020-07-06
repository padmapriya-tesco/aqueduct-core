package com.tesco.telemetry.micronaut;

import com.tesco.telemetry.jaeger.TescoCodec;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.tracing.instrument.http.AbstractOpenTracingFilter;
import org.reactivestreams.Publisher;
import org.slf4j.MDC;

@Filter(AbstractOpenTracingFilter.SERVER_PATH)
@Requires(classes = MDC.class)
public class CopyTraceIdToMdcServerFilter implements HttpServerFilter {

    private final com.tesco.telemetry.micronaut.CopyTraceIdToMdcConfig config;

    public CopyTraceIdToMdcServerFilter(com.tesco.telemetry.micronaut.CopyTraceIdToMdcConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(
        HttpRequest<?> request, ServerFilterChain chain) {
        if (!config.isEnabled() || !request.getHeaders().contains(TescoCodec.TRACE_ID_HEADER)) {
            return chain.proceed(request);
        }
        final String mdcKey = config.getMdcKey();
        final String traceId = request.getHeaders().get(TescoCodec.TRACE_ID_HEADER);
        MDC.put(mdcKey, traceId);
        return chain.proceed(request);
    }

    @Override
    public int getOrder() {
        return config.getOrder();
    }
}