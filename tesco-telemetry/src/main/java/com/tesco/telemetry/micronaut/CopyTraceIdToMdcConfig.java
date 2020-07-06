package com.tesco.telemetry.micronaut;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties(com.tesco.telemetry.micronaut.CopyTraceIdToMdcConfig.CONFIGURATION_PREFIX)
public interface CopyTraceIdToMdcConfig {
    String CONFIGURATION_PREFIX = "tesco.telemetry.trace-id-to-mdc";

    @Bindable(defaultValue = "trace_id")
    String getMdcKey();

    @Bindable(defaultValue = "true")
    boolean isEnabled();

    /**
     * Default value puts it BEFORE {@link DetailedOpenTracingServerFilter} but after {@link
     * TraceIdGeneratorFilter}.
     */
    @Bindable(defaultValue = "-150") // InterceptPhase.VALIDATE.getPosition() - 30;
    int getOrder();
}