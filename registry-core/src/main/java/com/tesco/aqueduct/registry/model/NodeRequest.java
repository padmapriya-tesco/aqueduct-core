package com.tesco.aqueduct.registry.model;

import lombok.Data;

@Data
public class NodeRequest {
    private final String hostId;
    private final Bootstrap bootstrap;
}
