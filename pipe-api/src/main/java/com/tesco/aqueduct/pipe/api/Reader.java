package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.OptionalLong;

public interface Reader {
    MessageResults read(List<String> types, long offset, final List<String> targetUuids);
    OptionalLong getOffset(OffsetName offsetName);
    PipeState getPipeState();
    default void runVisibilityCheck() {
        //null op
    };
}
