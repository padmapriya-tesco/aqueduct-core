package com.tesco.aqueduct.registry.model;

import lombok.Data;

import java.util.List;

@Data
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;
}
