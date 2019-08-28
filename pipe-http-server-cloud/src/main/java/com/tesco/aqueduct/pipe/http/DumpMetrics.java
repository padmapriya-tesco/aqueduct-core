package com.tesco.aqueduct.pipe.http;

import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint;
import io.micronaut.context.annotation.Context;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.SortedSet;

@Context
public class DumpMetrics {
    private static final Logger log = LoggerFactory.getLogger("metrics");

    @Inject
    private MetricsEndpoint metrics;

    @Scheduled(fixedDelay = "1m")
    public void dumpMetrics() {
        final SortedSet<String> names = metrics.listNames().getNames();
        names.stream().forEach(this::dumpMetric);
    }

    private void dumpMetric(final String metricName) {
        final MetricsEndpoint.MetricDetails details = metrics.getMetricDetails(metricName, null);
        dumpMetric(metricName, details);
        details.getAvailableTags().stream()
            .forEach(tag -> dumpTag(metricName, tag));
    }

    private void dumpTag(final String metricName, @NotNull final MetricsEndpoint.AvailableTag tag) {
        tag.getValues().stream()
            .map(v -> tag.getTag() + ":" + v)
            .forEach(tagValue ->
                dumpMetric(
                    metricName + ":" + tagValue,
                    metrics.getMetricDetails(metricName, Collections.singletonList(tagValue))
                )
            );
    }

    private void dumpMetric(final String metricName, final MetricsEndpoint.MetricDetails details) {
        details.getMeasurements().stream().forEach(sample -> {
            MDC.put("statistic", sample.getStatistic().name());
            MDC.put("value", String.format("%.2f", sample.getValue()));
        });

        log.info(metricName);

        MDC.clear();
    }
}
