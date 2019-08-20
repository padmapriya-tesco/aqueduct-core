package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public interface PipeStateProvider {
    PipeStateResponse getState(List<String> types, MessageReader reader);
}
