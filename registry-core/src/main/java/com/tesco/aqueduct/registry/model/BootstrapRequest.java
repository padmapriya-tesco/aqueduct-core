package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder(toBuilder = true)
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;
}
