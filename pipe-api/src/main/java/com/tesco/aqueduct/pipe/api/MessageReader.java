package com.tesco.aqueduct.pipe.api;

import java.util.List;

public interface MessageReader {
    MessageResults read(List<String> types, long offset);
    long getLatestOffsetMatching(List<String> types);
}
