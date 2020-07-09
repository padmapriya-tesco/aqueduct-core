package com.tesco.telemetry.micronaut;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("telemetry.trace-id-generator")
public interface TradeIdGeneratorConfig {

    @Bindable(defaultValue = "aq")
    String getPrefix();
}
