package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
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

import static java.util.Arrays.asList;

@Secured(SecurityRule.IS_AUTHENTICATED)
@Measure
@Controller
public class PipeReadController {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadController.class));

    //Using property injection until micronaut test framework is stable
    @Inject @Named("local")
    private MessageReader messageReader;

    @Value("${pipe.http.server.read.poll-seconds:0}")
    private int pollSeconds;

    @ReadableBytes @Value("${pipe.http.server.read.response-size-limit-in-bytes:1024kb}")
    private int maxPayloadSizeBytes;

    @Get("/pipe/offset/latest")
    long latestOffset(HttpRequest<?> request) {
        Map<String, List<String>> tags = parseTags(request);
        return messageReader.getLatestOffsetMatching(tags);
    }

    @Get("/pipe/{offset}")
    HttpResponse<List<Message>> readMessages(long offset, HttpRequest<?> request) {
        if(offset < 0) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request.getRemoteAddress().getHostName());

        Map<String, List<String>> tags = parseTags(request);
        LOG.withTags(tags).debug("pipe read controller", "reading with tags");

        val messageResults = messageReader.read(tags, offset);

        //val list = takeMessagesToSizeLimit(messageResults.getMessages(), maxPayloadSizeBytes);
        val list = messageResults.getMessages();

        long retryTime = messageResults.getRetryAfterSeconds();

        LOG.debug("pipe read controller", String.format("set retry time to %d", retryTime));

        return HttpResponse.ok(list)
                .header("Retry-After", String.valueOf(retryTime));
    }

    private void logOffsetRequestFromRemoteHost(long offset, String hostName) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("pipe read controller",
                    String.format(
                            "reading from offset %d, requested by %s", offset, hostName
                    )
            );
        }
    }

    /**
     * Query parameters with many values are supported by micronaut,
     * this method adds support for comma delimited parameters.
     */
    private Map<String, List<String>> parseTags(HttpRequest<?> request) {
        val result = new LinkedHashMap<String, List<String>>();

        // not the most efficient method
        request.getParameters().forEach( (k, values) -> {
            val list = new ArrayList<String>();
            values.forEach( v -> list.addAll(asList(v.split(","))));
            result.put(k, list);
        });

        return result;
    }
}
