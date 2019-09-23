package com.tesco.aqueduct.registry;

import io.micronaut.http.client.HttpClientConfiguration;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.reactivex.Flowable.fromIterable;

@Singleton
public class ServiceList {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(ServiceList.class));
    private final HttpClientConfiguration configuration;
    private List<PipeServiceInstance> services;
    private final PipeServiceInstance cloudInstance;

    @Inject
    public ServiceList(final HttpClientConfiguration configuration, final PipeServiceInstance pipeServiceInstance) {
        this.configuration = configuration;
        this.cloudInstance = pipeServiceInstance;
        defaultToCloud();
    }

    public ServiceList(final HttpClientConfiguration configuration, final PipeServiceInstance pipeServiceInstance, File file) throws IOException {
        this.configuration = configuration;
        this.cloudInstance = pipeServiceInstance;

        services = new ArrayList<>();
        readUrls(file);
    }

    private void readUrls(File file) throws IOException {
        if (!file.exists()) {
            defaultToCloud();
            return;
        }
        Properties properties = new Properties();
        try (FileInputStream stream = new FileInputStream(file)) {
            properties.load(stream);
        }
        update(readUrls(properties));
    }

    private List<URL> readUrls(Properties properties) {
        String urls = properties.getProperty("services", "");
        return Arrays.stream(urls.split(","))
            .map(this::toUrl)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private URL toUrl(String m) {
        try {
            return new URL(m);
        } catch (MalformedURLException e) {
            LOG.error("toUrl", "malformed url " + m, e.getMessage());
        }
        return null;
    }

    public void update(final List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            defaultToCloud();
            return;
        }
        services = urls.stream()
            .map(this::getServiceInstance)
            .collect(Collectors.toList());
        LOG.info("update services urls", servicesString());
    }

    private void defaultToCloud() {
        LOG.info("ServiceList.defaultToCloud", "Defaulting to follow the Cloud Pipe server.");
        this.services = new ArrayList<>();
        this.services.add(this.cloudInstance);
    }

    private PipeServiceInstance getServiceInstance(final URL url) {
        return findPreviousInstance(url)
            .orElseGet(() -> new PipeServiceInstance(configuration, url));
    }

    private Optional<PipeServiceInstance> findPreviousInstance(final URL url) {
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
            .map(PipeServiceInstance::getUrl)
            .map(URL::toString)
            .collect(Collectors.joining(","));
    }

    public void checkState() {
        LOG.info("ServiceList.checkState", "Urls:" + servicesString());
        fromIterable(services)
            .flatMapCompletable(PipeServiceInstance::checkState)
            .blockingAwait();
    }

    public Stream<PipeServiceInstance> stream() {
        return services.stream();
    }
}
