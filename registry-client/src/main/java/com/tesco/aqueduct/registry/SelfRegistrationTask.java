package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final RegistryHitList hitList;
    private final Supplier<Node> selfSummary;
    private final String cloudPipeUrl;

    private boolean hasRegistered;

    @Inject
    public SelfRegistrationTask(
        final RegistryClient client,
        final RegistryHitList hitList,
        @Named("selfSummarySupplier") final Supplier<Node> selfSummary,
        @Property(name = "pipe.http.client.url") final String cloudPipeUrl
    ) {
        this.client = client;
        this.hitList = hitList;
        this.selfSummary = selfSummary;
        this.cloudPipeUrl = cloudPipeUrl;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            final Node node = selfSummary.get();
            final List<URL> upstreamEndpoints = client.register(node);
            hitList.update(upstreamEndpoints);
            hasRegistered = true;
        } catch (Exception e) {
            LOG.error("registration scheduled task", "Register error", e);

            if (!hasRegistered) {
                defaultToFollowCloudPipe();
            }
        }
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
