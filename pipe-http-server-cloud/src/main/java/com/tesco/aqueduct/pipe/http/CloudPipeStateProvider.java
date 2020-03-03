package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.PipeState;

public class CloudPipeStateProvider implements PipeStateProvider {
    @Override
    public PipeState getState(MessageReader messageReader) {
        return PipeState.UP_TO_DATE;
    }
}
