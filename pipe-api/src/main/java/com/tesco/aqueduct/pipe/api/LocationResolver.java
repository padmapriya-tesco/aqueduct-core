package com.tesco.aqueduct.pipe.api;

import com.sun.istack.internal.NotNull;

import java.util.List;

public interface LocationResolver {
    List<Cluster> resolve(@NotNull String locationId);
}
