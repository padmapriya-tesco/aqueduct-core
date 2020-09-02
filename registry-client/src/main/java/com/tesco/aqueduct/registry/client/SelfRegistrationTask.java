package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.model.Bootstrapable;
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

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final SummarySupplier selfSummary;
    private final ServiceList services;
    private final Bootstrapable provider;
    private final Bootstrapable pipe;

    @Inject
    public SelfRegistrationTask(
        final RegistryClient client,
        final SummarySupplier selfSummary,
        final ServiceList services,
        @Named("provider") final Bootstrapable provider,
        @Named("pipe") final Bootstrapable pipe
    ) {
        this.client = client;
        this.selfSummary = selfSummary;
        this.services = services;
        this.provider = provider;
        this.pipe = pipe;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            final Node node = selfSummary.getSelfNode();
            final RegistryResponse registryResponse = client.register(node);
            if (registryResponse.getRequestedToFollow() == null) {
                LOG.error("SelfRegistrationTask.register", "Register error", "Null response received");
                return;
            }
            services.update(registryResponse.getRequestedToFollow());
            if (registryResponse.getBootstrapType() == BootstrapType.PROVIDER ) {
                provider.reset();
                provider.start();
            } else if(registryResponse.getBootstrapType() == BootstrapType.PIPE_AND_PROVIDER) {
                provider.reset();
                pipe.reset();
                pipe.start();
                provider.start();
            } else if (registryResponse.getBootstrapType() == BootstrapType.PIPE) {
                pipe.reset();
                pipe.start();
            }
        } catch (HttpClientResponseException hcre) {
            LOG.error("SelfRegistrationTask.register", "Register error [HttpClientResponseException]: %s", hcre.getMessage());
        } catch (Exception e) {
            LOG.error("SelfRegistrationTask.register", "Register error", e);
        }
    }
}
