package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.Reader;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public interface PipeStateProvider {
    PipeStateResponse getState(List<String> types, Reader reader);
}
