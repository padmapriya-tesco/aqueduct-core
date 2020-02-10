package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.PipeState;
import com.tesco.aqueduct.pipe.api.PipeStateResponse;

import java.util.List;

public class CloudPipeStateProvider implements PipeStateProvider {
    @Override
    public PipeStateResponse getState(List<String> types, MessageReader<MessageResults> reader) {
        return new PipeStateResponse(true, reader.getLatestOffsetMatching(types));
    }

    @Override
    public void setState(PipeState pipeState) { }

    @Override
    public PipeState getState() {
        return PipeState.UP_TO_DATE;
    }
}
