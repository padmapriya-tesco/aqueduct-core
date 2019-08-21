package com.tesco.aqueduct.pipe.api;

import lombok.Data;

@Data
public class PipeStateResponse {
    private boolean isUpToDate;
    private long localOffset;
}
