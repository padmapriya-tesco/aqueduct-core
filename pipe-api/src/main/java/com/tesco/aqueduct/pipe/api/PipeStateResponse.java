package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class PipeStateResponse {
    @JsonProperty
    private final boolean isUpToDate;

    @JsonProperty
    private final long localOffset;

    public PipeStateResponse(final boolean isUpToDate, final long localOffset) {
        this.isUpToDate = isUpToDate;
        this.localOffset = localOffset;
    }

    @JsonIgnore
    public boolean isUpToDate() {
        return isUpToDate;
    }

    @JsonIgnore
    public long getLocalOffset() {
        return localOffset;
    }
}
