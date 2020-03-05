package com.tesco.aqueduct.pipe.api;

public interface CentralStorage extends Reader {
    @Override
    default PipeState getPipeState() { return PipeState.UP_TO_DATE; }
}