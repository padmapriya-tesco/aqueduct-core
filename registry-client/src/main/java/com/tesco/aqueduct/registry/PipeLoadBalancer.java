package com.tesco.aqueduct.registry;

import io.micronaut.context.annotation.Property;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.discovery.ServiceInstance;
import io.micronaut.http.client.DefaultHttpClient;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.LoadBalancer;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.uri.UriBuilder;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.reactivex.Flowable.fromIterable;

@Singleton
public class PipeLoadBalancer implements LoadBalancer, RegistryHitList {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PipeLoadBalancer.class));
    private final HttpClientConfiguration configuration;

    private ServiceList services;

    @Inject
    PipeLoadBalancer(final HttpClientConfiguration configuration,
                    @Property(name = "pipe.http.client.url") final String cloudPipeUrl)
                    throws MalformedURLException {
        this.configuration = configuration;
        services = new ServiceList(configuration, cloudPipeUrl);
    }

    @Override
    public Publisher<ServiceInstance> select(@Nullable final Object discriminator) {
        return services.getServices().stream()
            .filter(PathRespectingPipeInstance::isUp)
            .findFirst()
            .map(ServiceInstance.class::cast)
            .map(Publishers::just)
            .orElse(Flowable.error(new RuntimeException("No accessible service to call.")));
    }

    @Override
    public void update(final List<URL> newUrls) {
        LOG.info("update services urls", servicesString());
        services.update(newUrls);
    }

    @Override
    public List<URL> getFollowing() {
        return services.getServices().stream()
            .filter(PathRespectingPipeInstance::isUp)
            .map(PathRespectingPipeInstance::getUrl)
            .collect(Collectors.toList());
    }

    public void recordError() {
        services.getServices().stream()
            .filter(PathRespectingPipeInstance::isUp)
            .findFirst()
            .ifPresent(instance -> instance.setUp(false));
    }

    public void checkState() {
        LOG.info("healthcheck urls", servicesString());
        fromIterable(services.getServices())
            .flatMapCompletable(PathRespectingPipeInstance::checkState)
            .blockingAwait();
    }

    private String servicesString() {
        return services.getServices().stream()
            .map(PathRespectingPipeInstance::getUrl)
            .map(URL::toString)
            .collect(Collectors.joining(","));
    }
}
