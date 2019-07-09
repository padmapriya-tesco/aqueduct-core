package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.registry.VersionChangedException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PostgresNodeGroup extends NodeGroup {
    private static final int UNPERSISTED_GROUP_VERSION = Integer.MIN_VALUE;
    private static final String QUERY_INSERT_GROUP =
            "INSERT INTO registry (group_id, entry, version)" +
                    "VALUES (" +
                    "?, " +
                    "?::JSON, " +
                    "0 " +
                    ")" +
                    "ON CONFLICT DO NOTHING ;";
    private static final String QUERY_UPDATE_GROUP =
            "UPDATE registry SET " +
                    "entry = ?::JSON , " +
                    "version = registry.version + 1 " +
                    "WHERE " +
                    "registry.group_id = ? " +
                    "AND " +
                    "registry.version = ? " +
                    ";";
    private static final String QUERY_DELETE_GROUP = "DELETE from registry where group_id = ? and version = ? ;";

    private final String groupId;
    private final int version;

    PostgresNodeGroup(String groupId) {
        super();
        this.groupId = groupId;
        this.version = UNPERSISTED_GROUP_VERSION;
    }

    PostgresNodeGroup(String groupId, int version, List<Node> nodes) {
        super(nodes);
        this.groupId = groupId;
        this.version = version;
    }

    public void insert(final Connection connection) throws IOException, SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_GROUP)) {
            statement.setString(1, groupId);
            statement.setString(2, nodesToJson());

            if (statement.executeUpdate() == 0) {
                //No rows updated
                throw new VersionChangedException();
            }
        }
    }

    public void update(final Connection connection) throws SQLException, IOException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_GROUP)) {
            statement.setString(1, nodesToJson());
            statement.setString(2, groupId);
            statement.setInt(3, version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }

    public void delete(final Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DELETE_GROUP)) {
            statement.setString(1, groupId);
            statement.setInt(2, version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }
}
