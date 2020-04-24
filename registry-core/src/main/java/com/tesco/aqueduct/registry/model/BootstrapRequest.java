package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> tillHosts;
    private final BootstrapType bootstrapType;

    public void save(NodeRequestStorage nodeRequestStorage) throws SQLException {
        for (String tillHost : tillHosts) {
            nodeRequestStorage.save(
                new NodeRequest(tillHost, new Bootstrap(bootstrapType, LocalDateTime.now()))
            );
        }
    }
}
