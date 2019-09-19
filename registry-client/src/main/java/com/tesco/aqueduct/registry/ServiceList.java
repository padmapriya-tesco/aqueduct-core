package com.tesco.aqueduct.registry;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.exceptions.HttpClientException;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ServiceList {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(ServiceList.class));
    private final HttpClientConfiguration configuration;
    private List<PathRespectingPipeInstance> services;
    private final PathRespectingPipeInstance cloudInstance;

    @Inject
    public ServiceList(final HttpClientConfiguration configuration,
                       @Property(name = "pipe.http.client.url") final String cloudPipeUrl) throws MalformedURLException {
        this(configuration, new URL(cloudPipeUrl));
    }

    public ServiceList(final HttpClientConfiguration configuration, final URL cloudPipeUrl) {
        this.configuration = configuration;
        this.cloudInstance = new PathRespectingPipeInstance(configuration, cloudPipeUrl, true);
        defaultToCloud();
    }

    public void update(List<URL> urls) {
        if (urls == null || urls.size() == 0) {
            defaultToCloud();
            return;
        }
        LOG.info("update services urls", servicesString());
        services = urls.stream()
            .map(this::getServiceInstance)
            .collect(Collectors.toList());
    }

    private void defaultToCloud() {
        LOG.info("ServiceList.defaultToCloud", "Defaulting to follow the Cloud Pipe server.");
        this.services = new ArrayList<>();
        this.services.add(this.cloudInstance);
    }

    private PathRespectingPipeInstance getServiceInstance(final URL url) {
        return findPreviousInstance(url)
            .orElseGet(() -> new PathRespectingPipeInstance(configuration, url, true));
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

    private String servicesString() {
        return services.stream()
            .map(PathRespectingPipeInstance::getUrl)
            .map(URL::toString)
            .collect(Collectors.joining(","));
    }

    public List<PathRespectingPipeInstance> getServices() {
        return services;
    }

}
