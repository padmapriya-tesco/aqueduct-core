package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;

import java.util.List;

public interface PipeStateProvider {
    PipeStateResponse getState(List<String> types, MessageReader reader);
}
