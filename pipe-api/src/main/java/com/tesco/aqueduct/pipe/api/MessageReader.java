package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.OptionalLong;

public interface MessageReader {
    MessageResults read(List<String> types, long offset, final String locationUuid);
    long getLatestOffsetMatching(List<String> types);
    OptionalLong getLatestOffset(OffsetName offsetName);
}
