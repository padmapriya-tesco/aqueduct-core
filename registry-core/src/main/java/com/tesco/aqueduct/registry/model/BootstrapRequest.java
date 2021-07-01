package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry;
import com.tesco.aqueduct.registry.postgres.PostgresNodeGroup;
import lombok.Data;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BootstrapRequest {
    private final List<String> locations;
    private final List<String> nodeRequests;
    private final BootstrapType bootstrapType;

    public void save(NodeRequestStorage nodeRequestStorage, PostgreSQLNodeRegistry postgreSQLNodeRegistry) throws SQLException {
        retrieveNodesForLocations(postgreSQLNodeRegistry);

        for (String nodeRequest : nodeRequests) {
            nodeRequestStorage.save(
                new NodeRequest(nodeRequest, new Bootstrap(bootstrapType, LocalDateTime.now()))
            );
        }
    }

    private void retrieveNodesForLocations(PostgreSQLNodeRegistry postgreSQLNodeRegistry) {
        if (!locations.isEmpty()) {
            List<PostgresNodeGroup> postgresNodeGroups = postgreSQLNodeRegistry.getPostgresNodeGroups(locations);

            for (PostgresNodeGroup postgresNodeGroup : postgresNodeGroups) {
                nodeRequests.addAll(postgresNodeGroup.getNodes().stream().map(node -> node.getPipe().get("host")).collect(Collectors.toSet()));
            }
        }
    }
}
