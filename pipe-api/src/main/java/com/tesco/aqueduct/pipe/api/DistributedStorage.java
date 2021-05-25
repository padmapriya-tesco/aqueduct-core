package com.tesco.aqueduct.pipe.api;

import java.util.List;

public interface DistributedStorage extends Reader, Writer {
    Long getMaxOffsetForConsumers(List<String> types);
}
