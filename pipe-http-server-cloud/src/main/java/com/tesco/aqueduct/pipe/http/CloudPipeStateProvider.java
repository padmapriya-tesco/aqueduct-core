package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.OffsetName;
import com.tesco.aqueduct.pipe.api.Reader;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public class CloudPipeStateProvider implements PipeStateProvider {
    @Override
    public PipeStateResponse getState(List<String> types, Reader reader) {
        return new PipeStateResponse(true, 0L);
    }
}
