package com.tesco.aqueduct.registry.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.NodeRegistry;
import com.tesco.aqueduct.registry.RegistryLogger;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.StateSummary;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

@Measure
@Controller("/v1/registry")
public class NodeRegistryControllerV1 {
    private static final String REGISTRY_DELETE = "REGISTRY_DELETE";
    private final NodeRegistry registry;
    private final MessageReader pipe;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(NodeRegistryControllerV1.class));

    public NodeRegistryControllerV1(final NodeRegistry registry, final MessageReader pipe) {
        this.registry = registry;
        this.pipe = pipe;
    }

    @Secured(SecurityRule.IS_ANONYMOUS)
    @Get
    public StateSummary getSummary(@Nullable final List<String> groups) {
        return registry.getSummary(pipe.getLatestOffsetMatching(null), "ok", groups);
    }

    @Secured(SecurityRule.IS_AUTHENTICATED)
    @Post
    public List<URL> registerNode(@Body final Node node) {
        final List<URL> requestedToFollow = registry.register(node);
        final String followStr = requestedToFollow.stream().map(URL::toString).collect(Collectors.joining(","));
        LOG.withNode(node).info("requested to follow", followStr);
        return requestedToFollow;
    }

    @Secured(REGISTRY_DELETE)
    @Delete("/{group}/{id}")
    public HttpResponse deleteNode(final String group, final String id) {
        final boolean deleted = registry.deleteNode(group, id);
        if (deleted) {
            return HttpResponse.status(HttpStatus.OK);
        } else {
            return HttpResponse.status(HttpStatus.NOT_FOUND);
        }
    }
}