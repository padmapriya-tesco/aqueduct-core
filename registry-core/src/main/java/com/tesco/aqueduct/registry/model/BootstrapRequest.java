package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.tesco.aqueduct.registry.TillStorage;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;

    public void save(TillStorage tillStorage) {
        tillHosts.forEach(tillHost ->
            tillStorage.updateTill(
                new Till(tillHost, new Bootstrap(bootstrapType, LocalDateTime.now()))
            )
        );
    }
}
