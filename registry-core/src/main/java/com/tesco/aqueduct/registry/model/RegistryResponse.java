package com.tesco.aqueduct.registry.model;

import lombok.Data;

import java.net.URL;
import java.util.List;

@Data
public class RegistryResponse {
    private final List<URL> requestedToFollow;
    private final BootstrapType bootstrapType;
}
