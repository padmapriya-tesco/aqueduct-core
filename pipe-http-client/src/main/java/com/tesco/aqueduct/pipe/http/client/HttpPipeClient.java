package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.registry.PipeLoadBalancer;
import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Named("remote")
public class HttpPipeClient implements MessageReader {

    private final InternalHttpPipeClient client;
    private final PipeLoadBalancer pipeLoadBalancer;

    @Inject
    public HttpPipeClient(InternalHttpPipeClient client, PipeLoadBalancer pipeLoadBalancer) {
        this.client = client;
        this.pipeLoadBalancer = pipeLoadBalancer;
    }

    @Override
    public MessageResults read(@Nullable Map<String, List<String>> tags, long offset) {
        try {
            return httpRead(tags, offset);
        } catch (Exception e) {
            pipeLoadBalancer.recordError();
            throw e;
        }
    }

    private MessageResults httpRead(@Nullable Map<String, List<String>> tags, long offset) {
        HttpResponse<List<Message>> response = client.httpRead(tags, offset);

        long retryAfter = Optional
            .ofNullable(response.header("Retry-After"))
            .map(value -> {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException exception) {
                    return 0L;
                }
            })
            .map(value -> Long.max(0, value))
            .orElse(0L);

        return new MessageResults(response.body(), retryAfter);
    }

    @Override
    public long getLatestOffsetMatching(Map<String, List<String>> tags) {
        return client.getLatestOffsetMatching(tags);
    }
}
