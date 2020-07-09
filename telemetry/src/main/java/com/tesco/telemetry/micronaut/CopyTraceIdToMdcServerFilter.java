package com.tesco.telemetry.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.tracing.instrument.http.AbstractOpenTracingFilter;
import org.reactivestreams.Publisher;
import org.slf4j.MDC;

import java.util.UUID;

@Filter(AbstractOpenTracingFilter.SERVER_PATH)
@Requires(classes = MDC.class)
public class CopyTraceIdToMdcServerFilter implements HttpServerFilter {

    private final com.tesco.telemetry.micronaut.CopyTraceIdToMdcConfig config;
    private final TradeIdGeneratorConfig traceIdGeneratorConfig;

    public CopyTraceIdToMdcServerFilter(
            com.tesco.telemetry.micronaut.CopyTraceIdToMdcConfig copyTraceIdToMdcConfig,
            TradeIdGeneratorConfig traceIdGeneratorConfig
        ) {
            this.config = copyTraceIdToMdcConfig;
            this.traceIdGeneratorConfig = traceIdGeneratorConfig;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(
            HttpRequest<?> request,
            ServerFilterChain chain
        ) {
            if (!config.isEnabled()) {
                return chain.proceed(request);
            }

            if (request.getHeaders().contains("TraceId")) {
                return addTraceIdToMdc(request, chain, request.getHeaders().get("TraceId"));
            } else {
                return addTraceIdToMdc(request, chain, getGeneratedTraceId());
            }
    }

    private Publisher<MutableHttpResponse<?>> addTraceIdToMdc(
            HttpRequest<?> request,
            ServerFilterChain chain,
            String traceId
    ) {
        final String mdcKey = config.getMdcKey();
        MDC.put(mdcKey, traceId);
        return chain.proceed(request);
    }

    private String getGeneratedTraceId() {
        return traceIdGeneratorConfig.getPrefix() + "-" + UUID.randomUUID().toString();
    }

    @Override
    public int getOrder() {
        return config.getOrder();
    }
}