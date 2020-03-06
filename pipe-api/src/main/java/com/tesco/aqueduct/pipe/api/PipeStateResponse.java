package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

@Data
public class PipeStateResponse {
    private final boolean upToDate;
    @JsonSerialize(using = ToStringSerializer.class)
    private final long localOffset;
}
