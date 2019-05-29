package com.tesco.aqueduct.registry;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final RegistryHitList hitList;
    private final Supplier<Node> selfSummary;
    private final String cloudPipeUrl;
    private final Supplier<Map<String, Object>> providerMetricsSupplier;

    private boolean hasRegistered;

    @Inject
    public SelfRegistrationTask(
        RegistryClient client,
        RegistryHitList hitList,
        @Named("selfSummarySupplier") Supplier<Node> selfSummary,
        @Named("providerMetricsSupplier") Supplier<Map<String, Object>> providerMetricsSupplier,
        @Property(name = "pipe.http.client.url") String cloudPipeUrl
    ) {
        this.client = client;
        this.hitList = hitList;
        this.selfSummary = selfSummary;
        this.providerMetricsSupplier = providerMetricsSupplier;
        this.cloudPipeUrl = cloudPipeUrl;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            Node node = getMetrics();
            List<URL> upstreamEndpoints = client.register(node);
            hitList.update(upstreamEndpoints);
            hasRegistered = true;
        } catch (Exception e) {
            LOG.error("registration scheduled task", "Register error", e);

            if (!hasRegistered) {
                defaultToFollowCloudPipe();
            }
        }
     }

    private Node getMetrics() {
        Node.NodeBuilder nodeBuilder = selfSummary.get().toBuilder();

        Map<String, Object> providerMetrics = providerMetricsSupplier.get();
        if (providerMetrics != null) {
            long providerLastAckedOffset = (long) providerMetrics.getOrDefault("lastAckedOffset", 0L);
            nodeBuilder.providerLastAckOffset(providerLastAckedOffset);

            String timeOfLastAck = (String) providerMetrics.get("timeOfLastAck");
            if (timeOfLastAck != null) {
                ZonedDateTime time = ZonedDateTime.parse(timeOfLastAck);
                nodeBuilder.providerLastAckTime(time);
            }
        }
        return nodeBuilder.build();
    }

    private void defaultToFollowCloudPipe() {
        try {
            LOG.info("registration scheduled task", "Defaulting to follow the Cloud Pipe server.");
            hitList.update(Collections.singletonList(new URL(cloudPipeUrl)));

        } catch (MalformedURLException e) {
            LOG.error("registration scheduled task", "Invalid Cloud Pipe URL.", e);
        }
     }
}
