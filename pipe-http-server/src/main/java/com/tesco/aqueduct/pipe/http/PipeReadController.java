package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.convert.format.ReadableBytes;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import lombok.val;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Secured(SecurityRule.IS_AUTHENTICATED)
@Measure
@Controller
public class PipeReadController {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadController.class));

    //TODO: Use constructor
    @Inject @Named("local")
    private MessageReader messageReader;

    @Inject
    private PipeStateProvider pipeStateProvider;

    @Value("${pipe.http.server.read.poll-seconds:0}")
    private int pollSeconds;

    @ReadableBytes @Value("${pipe.http.server.read.response-size-limit-in-bytes:1024kb}")
    private int maxPayloadSizeBytes;

    @Get("/pipe/offset/latest{?type}")
    public long latestOffset(final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return messageReader.getLatestOffsetMatching(types);
    }

    @Get("/pipe/state{?type}")
    public PipeStateResponse state(final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return pipeStateProvider.getState(types, messageReader);
    }

    @Get("/pipe/{offset}{?type}")
    public HttpResponse<List<Message>> readMessages(final long offset, final HttpRequest<?> request) {
        if(offset < 0) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request.getRemoteAddress().getHostName());

        final List<String> types = flattenRequestParams(request.getParameters().getAll("type"));
        LOG.withTypes(types).debug("pipe read controller", "reading with types");

        final val messageResults = messageReader.read(types, offset);

        //val list = takeMessagesToSizeLimit(messageResults.getMessages(), maxPayloadSizeBytes);
        final val list = messageResults.getMessages();

        final long retryTime = messageResults.getRetryAfterSeconds();

        LOG.debug("pipe read controller", String.format("set retry time to %d", retryTime));

        return HttpResponse.ok(list).header("Retry-After", String.valueOf(retryTime));
    }

    private void logOffsetRequestFromRemoteHost(final long offset, final String hostName) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("pipe read controller",
                    String.format(
                            "reading from offset %d, requested by %s", offset, hostName
                    )
            );
        }
    }

    private List<String> flattenRequestParams(final List<String> strings) {
        if(strings == null) {
            return Collections.emptyList();
        }
        return strings
            .stream()
            .flatMap(s -> Stream.of(s.split(",")))
            .collect(Collectors.toList());
    }
}
