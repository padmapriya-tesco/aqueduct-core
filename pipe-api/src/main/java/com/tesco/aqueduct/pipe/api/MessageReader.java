package com.tesco.aqueduct.pipe.api;

import java.util.List;

public interface MessageReader<RESULTS> {
    RESULTS read(List<String> types, long offset, final String locationUuid);
    long getLatestOffsetMatching(List<String> types);
}
