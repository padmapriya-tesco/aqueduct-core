package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.PipeState;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public interface PipeStateProvider {
    @Deprecated
    PipeStateResponse getState(List<String> types, MessageReader<MessageResults> reader);
    void setState(PipeState pipeState);
    PipeState getState();
}
