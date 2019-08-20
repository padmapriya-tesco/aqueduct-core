package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
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
