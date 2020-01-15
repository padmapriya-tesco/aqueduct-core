package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;
import io.micronaut.cache.CacheManager;
import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;

@Named("remote")
public class HttpPipeClient implements MessageReader {

    private final InternalHttpPipeClient client;
    private final CacheManager cacheManager;

    @Inject
    public HttpPipeClient(final InternalHttpPipeClient client, final CacheManager cacheManager) {
        this.client = client;
        this.cacheManager = cacheManager;
    }

    @Override
    public MessageResults read(@Nullable final List<String> types, final long offset, final String locationUuid) {
        final HttpResponse<List<Message>> response = client.httpRead(types, offset, locationUuid);

        final long retryAfter = Optional
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
    public long getLatestOffsetMatching(final List<String> types) {
        return client.getLatestOffsetMatching(types);
    }

    public PipeStateResponse getPipeState(final List<String> type) {
        final PipeStateResponse pipeState = client.getPipeState(type);
        if (!pipeState.isUpToDate()) {
            cacheManager.getCache("health-check").invalidateAll();
        }
        return pipeState;
    }

}
