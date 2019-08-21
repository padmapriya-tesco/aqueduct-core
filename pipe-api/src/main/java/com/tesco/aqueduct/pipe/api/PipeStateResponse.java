package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class PipeStateResponse {
    @Getter
    @JsonProperty
    private final boolean isUpToDate;

    @Getter
    @JsonProperty
    private final long localOffset;

    public PipeStateResponse(final boolean isUpToDate, final long localOffset) {
        this.isUpToDate = isUpToDate;
        this.localOffset = localOffset;
    }
}
