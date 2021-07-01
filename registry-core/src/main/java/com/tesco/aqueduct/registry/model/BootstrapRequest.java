package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> locations;
    private final List<String> nodeRequests;
    private final BootstrapType bootstrapType;

    public void save(NodeRequestStorage nodeRequestStorage, NodeRegistry nodeRegistry) throws SQLException {
        retrieveNodesForLocations(nodeRegistry);

        for (String nodeRequest : nodeRequests) {
            nodeRequestStorage.save(
                new NodeRequest(nodeRequest, new Bootstrap(bootstrapType, LocalDateTime.now()))
            );
        }
    }

    private void retrieveNodesForLocations(NodeRegistry nodeRegistry) {
        if (!locations.isEmpty()) {
            nodeRequests.addAll(nodeRegistry.getNodeHostsForGroups(locations));
        }
    }
}
