package com.tesco.telemetry.micronaut

import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.filter.ServerFilterChain
import org.slf4j.MDC
import spock.lang.Specification

class CopyTraceIdToMdcServerFilterSpec extends Specification {

    void cleanup() {
        MDC.clear()
    }

    def "if TraceId does not exist in header it should generate one with a configured prefix"() {
        given: "correct configs"
        def copyTraceIdToMdcConfig = Mock(CopyTraceIdToMdcConfig) {
            getMdcKey() >> "trace_id"
            isEnabled() >> true
        }
        def traceIdGeneratorConfig = Mock(TradeIdGeneratorConfig) {
            getPrefix() >> "aq"
        }

        and: "a request and service filter chain"
        def headers = Mock (HttpHeaders) {
            contains("trace_id") >> false
        }
        def httpRequest = Mock(HttpRequest) {
            getHeaders() >> headers
        }
        def serverFilterChain = Mock(ServerFilterChain)

        and: "a filter"
        def filter = new CopyTraceIdToMdcServerFilter(copyTraceIdToMdcConfig, traceIdGeneratorConfig)

        when: "do filter is called"
        filter.doFilter(httpRequest, serverFilterChain)

        then: "MDC contains a generated trace_id with correct prefix"
        MDC.get("trace_id").startsWith("aq-")
    }

    def "if config is disabled TraceId should not be put in MDC"() {
        given: "correct configs"
        def copyTraceIdToMdcConfig = Mock(CopyTraceIdToMdcConfig) {
            getMdcKey() >> "trace_id"
            isEnabled() >> false
        }
        def traceIdGeneratorConfig = Mock(TradeIdGeneratorConfig)

        and: "a request and a service filter chain"
        def httpRequest = Mock(HttpRequest)
        def serverFilterChain = Mock(ServerFilterChain)

        and: "a filter"
        def filter = new CopyTraceIdToMdcServerFilter(copyTraceIdToMdcConfig, traceIdGeneratorConfig)

        when: "do filter is called"
        filter.doFilter(httpRequest, serverFilterChain)

        then: "MDC doesn't contain a trace_id"
        MDC.get("trace_id") == null
    }

    def "if TraceId exists in a header it should return that TraceId"() {
        given: "correct configs"
        def copyTraceIdToMdcConfig = Mock(CopyTraceIdToMdcConfig) {
            getMdcKey() >> "trace_id"
            isEnabled() >> true
        }
        def traceIdGeneratorConfig = Mock(TradeIdGeneratorConfig)

        and: "a request and a service filter chain"
        def headers = Mock (HttpHeaders) {
            contains("TraceId") >> true
            get("TraceId") >> "someTraceId"
        }
        def httpRequest = Mock(HttpRequest) {
            getHeaders() >> headers
        }
        def serverFilterChain = Mock(ServerFilterChain)

        and: "a filter"
        def filter = new CopyTraceIdToMdcServerFilter(copyTraceIdToMdcConfig, traceIdGeneratorConfig)

        when: "do filter is called"
        filter.doFilter(httpRequest, serverFilterChain)

        then: "MDC contains the trace id"
        MDC.get("trace_id") == "someTraceId"
    }
}
