package com.tesco.aqueduct.registry;

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

    private List<PathRespectingPipeInstance> services;

    @Inject
    PipeLoadBalancer(final HttpClientConfiguration configuration){
        this.configuration = configuration;
        services = Collections.emptyList();
    }

    @Override
    public Publisher<ServiceInstance> select(@Nullable final Object discriminator) {
        return services.stream()
            .filter(PathRespectingPipeInstance::isUp)
            .findFirst()
            .map(ServiceInstance.class::cast)
            .map(Publishers::just)
            .orElse(Flowable.error(new RuntimeException("No accessible service to call.")));
    }

    @Override
    public void update(final List<URL> newUrls) {
        LOG.info("update services urls", servicesString());
        services = newUrls.stream()
            .map(this::newServiceInstance)
            .collect(Collectors.toList());
    }

    private PathRespectingPipeInstance newServiceInstance(final URL url) {
        return
            findPreviousInstance(url)
            .map(oldInstance -> new PathRespectingPipeInstance(url, oldInstance.isUp()))
            .orElseGet(() -> new PathRespectingPipeInstance(url, true));
    }

    @Override
    public List<URL> getFollowing() {
        return services.stream()
            .filter(PathRespectingPipeInstance::isUp)
            .map(PathRespectingPipeInstance::getUrl)
            .collect(Collectors.toList());
    }

    public void recordError() {
        services.stream()
            .filter(PathRespectingPipeInstance::isUp)
            .findFirst()
            .ifPresent(instance -> instance.setUp(false));
    }

    private Optional<PathRespectingPipeInstance> findPreviousInstance(final URL url) {
        try {
            // We have to use URIs for this comparison as URLs are converted to IPs under the hood, which causes issues
            // for local testing
            final URI uri = url.toURI();
            return services.stream()
                .filter(oldInstance -> uri.equals(oldInstance.getURI()))
                .findFirst();
        } catch (URISyntaxException exception) {
            LOG.error("pipe load balancer", "invalid URI", exception);
            return Optional.empty();
        }
    }

    public void checkState() {
        LOG.info("healthcheck urls", servicesString());
        fromIterable(services)
            .flatMapCompletable(instance -> checkState(new DefaultHttpClient(instance.getUrl(), configuration), instance))
            .blockingAwait();
    }

    private Completable checkState(final RxHttpClient client, final PathRespectingPipeInstance instance) {
        final String urlWithPath = UriBuilder.of(instance.getURI()).path("/pipe/_status").build().toString();
        return client.retrieve(urlWithPath)
            // if got response, then it's a true
            .map(response -> true )

            // log result
            .doOnNext(b -> LOG.info("healthcheck.success", instance.getUrl().toString()))
            .doOnError(t -> LOG.error("healthcheck.failed", instance.getUrl().toString(), t))

            // change exception to "false"
            .onErrorResumeNext(Flowable.just(false))

            // set the status of the instance
            .doOnNext(instance::setUp)

            // return as completable, close client and ignore any errors
            .ignoreElements() // returns completable
            .doOnComplete(client::close)
            .onErrorComplete();
    }

    private String servicesString() {
        return services.stream()
            .map(PathRespectingPipeInstance::getUrl)
            .map(URL::toString)
            .collect(Collectors.joining(","));
    }
}
