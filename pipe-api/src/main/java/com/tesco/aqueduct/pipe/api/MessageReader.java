package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.Map;

public interface MessageReader {
    MessageResults read(Map<String, List<String>> tags, long offset);
    long getLatestOffsetMatching(Map<String, List<String>> tags);
}
