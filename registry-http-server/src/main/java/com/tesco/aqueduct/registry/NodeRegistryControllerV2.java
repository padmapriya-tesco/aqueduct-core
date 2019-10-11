package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.model.*;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Measure
@Controller("/v2/registry")
public class NodeRegistryControllerV2 {
    private static final String REGISTRY_DELETE = "REGISTRY_DELETE";
    private static final String BOOTSTRAP_TILL = "BOOTSTRAP_TILL";

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(NodeRegistryControllerV2.class));

    @Inject
    private NodeRegistry registry;

    @Inject
    private TillStorage tillStorage;

    // This is temporary, it might be better for us to make pipe depend on registry and have it register itself in it.
    @Inject
    private MessageReader pipe;

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

    @Secured(BOOTSTRAP_TILL)
    @Post("/bootstrap")
    public HttpResponse bootstrap(@Body final BootstrapRequest bootstrapRequest) {
        bootstrapRequest.getTillHosts().forEach(tillHost ->
            tillStorage.updateTill(
                new Till(
                    tillHost,
                    new Bootstrap(
                        bootstrapRequest.getBootstrapType(),
                        LocalDateTime.now()
                    )
                )
        ));

        return HttpResponse.status(HttpStatus.OK);
    }
}