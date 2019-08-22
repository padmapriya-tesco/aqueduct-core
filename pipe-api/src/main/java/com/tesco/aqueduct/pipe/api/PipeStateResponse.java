package com.tesco.aqueduct.pipe.api;

import lombok.Data;

@Data
public class PipeStateResponse {
    private final boolean upToDate;
    private final long localOffset;
}
