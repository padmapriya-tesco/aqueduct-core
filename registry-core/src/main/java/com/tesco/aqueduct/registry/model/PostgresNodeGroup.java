package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.registry.VersionChangedException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PostgresNodeGroup extends NodeGroup {
    private static final String QUERY_DELETE_GROUP = "DELETE from registry where group_id = ? and version = ? ;";

    private String groupId;

    PostgresNodeGroup() {
        super();
    }

    PostgresNodeGroup(String groupId, int version, List<Node> nodes) {
        super(nodes, version);
        this.groupId = groupId;
    }

    public void delete(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DELETE_GROUP)) {
            statement.setString(1, groupId);
            statement.setInt(2, version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }
}
