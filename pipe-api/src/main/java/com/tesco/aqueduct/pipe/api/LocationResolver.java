package com.tesco.aqueduct.pipe.api;

import java.util.List;

public interface LocationResolver {
    List<String> resolve(String location);
}
