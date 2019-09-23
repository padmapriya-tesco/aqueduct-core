package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URL;
import java.util.List;
import java.util.function.Supplier;

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final Supplier<Node> selfSummary;
    private final ServiceList services;

    @Inject
    public SelfRegistrationTask(
        final RegistryClient client,
        @Named("selfSummarySupplier") final Supplier<Node> selfSummary,
        final ServiceList services
    ) {
        this.client = client;
        this.selfSummary = selfSummary;
        this.services = services;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            final Node node = selfSummary.get();
            final List<URL> upstreamEndpoints = client.register(node);
            if (upstreamEndpoints == null) {
                LOG.error("SelfRegistrationTask.register", "Register error", "Null response received");
                return;
            }
            services.update(upstreamEndpoints);
        } catch (Exception e) {
            LOG.error("SelfRegistrationTask.register", "Register error", e);
        }
    }
}
