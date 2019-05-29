package com.tesco.aqueduct.registry;

import io.micronaut.discovery.ServiceInstance;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class PathRespectingPipeInstance implements ServiceInstance {

    private final URL url;
    private boolean up;

    public PathRespectingPipeInstance(URL url, boolean up) {
        this.url = url;
        this.up = up;
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(boolean isServiceUp) {
        up = isServiceUp;
    }

    public URL getUrl() {
        return url;
    }

    @Override
    public String getId() {
        return "pipe";
    }

    @Override
    public URI getURI() {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public URI resolve(URI relativeURI) {
        try {
            if (url.getPath() != null && !url.getPath().isEmpty()) {
                relativeURI = getUriWithBasePath(relativeURI);
            }
            return ServiceInstance.super.resolve(relativeURI);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("ServiceInstance URI is invalid: " + e.getMessage(), e);
        }
    }

    private URI getUriWithBasePath(URI relativeURI) throws URISyntaxException {
        String path = Paths.get(url.getPath(), relativeURI.getPath()).toString();
        relativeURI = new URI(
            relativeURI.getScheme(),
            relativeURI.getUserInfo(),
            relativeURI.getHost(),
            relativeURI.getPort(),
            path,
            relativeURI.getQuery(),
            relativeURI.getFragment()
        );
        return relativeURI;
    }
}
