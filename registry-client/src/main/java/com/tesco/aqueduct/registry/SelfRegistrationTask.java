package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.RegistryResponse;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
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
            final RegistryResponse registryResponse = client.register(node);
            if (registryResponse.getRequestedToFollow() == null) {
                LOG.error("SelfRegistrationTask.register", "Register error", "Null response received");
                return;
            }
            services.update(registryResponse.getRequestedToFollow());
        } catch (HttpClientResponseException hcre) {
            LOG.error("SelfRegistrationTask.register", "Register error [HttpClientResponseException]: %s", hcre.getMessage());
        } catch (Exception e) {
            LOG.error("SelfRegistrationTask.register", "Register error", e);
        }
    }
}
