package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class PipeStateResponse {
    @JsonProperty
    private boolean isUpToDate;

    @JsonProperty
    private long localOffset;

    public PipeStateResponse(final boolean isUpToDate, final long localOffset) {
        this.isUpToDate = isUpToDate;
        this.localOffset = localOffset;
    }
}
