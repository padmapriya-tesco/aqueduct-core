package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;
}
