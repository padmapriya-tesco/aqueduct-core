package com.tesco.aqueduct.registry.postgres;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresNodeGroupStorage {
    private static final String QUERY_GET_GROUP_BY_ID = "SELECT group_id, entry, version FROM registry where group_id = ? FOR UPDATE;";
    private static final String QUERY_GET_ALL_GROUPS = "SELECT group_id, entry, version FROM registry ORDER BY group_id";

    PostgresNodeGroupStorage() { }

    PostgresNodeGroup getNodeGroup(final Connection connection, final String groupId) throws SQLException, IOException {

        try(PreparedStatement beginStatement = connection.prepareStatement("BEGIN WORK;")) {
            beginStatement.execute();

            try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_GROUP_BY_ID)) {
                statement.setString(1, groupId);

                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return PostgresNodeGroup.createNodeGroup(rs);
                    } else {
                        return new PostgresNodeGroup(groupId);
                    }
                }
            }

        }
    }

    List<PostgresNodeGroup> getNodeGroups(final Connection connection, final List<String> groupIds) throws SQLException, IOException {
        if (groupIds == null || groupIds.isEmpty()) {
            return getAllNodeGroups(connection);
        }

        final List<PostgresNodeGroup> list = new ArrayList<>();
        for (final String group : groupIds) {
            list.add(getNodeGroup(connection, group));
        }
        return list;
    }

    private List<PostgresNodeGroup> getAllNodeGroups(final Connection connection) throws SQLException, IOException {
        List<PostgresNodeGroup> groups;
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_ALL_GROUPS)) {
            groups = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    groups.add(PostgresNodeGroup.createNodeGroup(rs));
                }
            }
        }
        return groups;
    }
}
