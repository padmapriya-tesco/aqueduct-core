package com.tesco.aqueduct.pipe.api;

public interface MessageStorage extends MessageReader<MessageResults>, MessageWriter {
    void write(PipeState pipeState);
}
