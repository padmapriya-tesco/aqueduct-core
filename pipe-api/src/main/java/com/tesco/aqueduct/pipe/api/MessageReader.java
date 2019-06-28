package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.Map;

public interface MessageReader {
    MessageResults read(List<String> types, long offset);
    long getLatestOffsetMatching(List<String> types);
}
