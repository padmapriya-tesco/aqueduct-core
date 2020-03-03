package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.PipeState;

public interface PipeStateProvider {
    PipeState getState(MessageReader messageReader);
}
