package com.tesco.aqueduct.pipe.http;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipeStateResponse {
    @JsonProperty
    private final boolean isUpToDate;

    @JsonProperty
    private final long offset;

    public PipeStateResponse(final boolean isUpToDate, final long offset) {
        this.isUpToDate = isUpToDate;
        this.offset = offset;
    }
}
