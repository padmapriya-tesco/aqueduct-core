package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.*;
import io.micronaut.cache.CacheManager;
import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

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
            .ofNullable(response.header(HttpHeaders.RETRY_AFTER))
            .map(value -> {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException exception) {
                    return 0L;
                }
            })
            .map(value -> Long.max(0, value))
            .orElse(0L);

        return new MessageResults(
            response.body(),
            retryAfter,
            OptionalLong.of(getLatestGlobalOffset(types, response)),
            getPipeState(types, response)
        );
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        throw new UnsupportedOperationException("HttpPipeClient does not support this operation");
    }

    private long getLatestGlobalOffset(@Nullable List<String> types, HttpResponse<List<Message>> response) {
        // TODO - Ensure backwards compatible, need to update to throw error once all tills have latest software
        return getGlobalOffsetHeader(response) == null
            ? getLatestOffsetMatching(types)
            : Long.parseLong(getGlobalOffsetHeader(response));
    }

    private String getGlobalOffsetHeader(HttpResponse<List<Message>> response) {
        return response.header(HttpHeaders.GLOBAL_LATEST_OFFSET);
    }

    private PipeState getPipeState(@Nullable List<String> types, HttpResponse<List<Message>> response) {
        return response.header(HttpHeaders.PIPE_STATE) == null
            ? getPipeState(types)
            : PipeState.valueOf(response.header(HttpHeaders.PIPE_STATE));
    }

    private PipeState getPipeState(@Nullable List<String> types) {
        return getPipeStateResponse(types).isUpToDate() ? PipeState.UP_TO_DATE : PipeState.OUT_OF_DATE;
    }

    @Override
    public long getLatestOffsetMatching(final List<String> types) {
        return client.getLatestOffsetMatching(types);
    }

    // This exists for backwards compatibility until we start using pipe state in header in till estate
    public PipeStateResponse getPipeStateResponse(final List<String> types) {
        final PipeStateResponse pipeState = client.getPipeState(types);
        if (!pipeState.isUpToDate()) {
            cacheManager.getCache("health-check").invalidateAll();
        }
        return pipeState;
    }
}
