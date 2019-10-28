package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.postgres.PostgresNodeGroup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresNodeGroupFactory {
    private static final String QUERY_GET_GROUP_BY_ID = "SELECT group_id, entry, version FROM registry where group_id = ? ;";
    private static final String QUERY_GET_ALL_GROUPS = "SELECT group_id, entry, version FROM registry ORDER BY group_id";

    public PostgresNodeGroupFactory() { }

    public PostgresNodeGroup getNodeGroup(final Connection connection, final String groupId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_GROUP_BY_ID)) {
            statement.setString(1, groupId);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return PostgresNodeGroup.createNodeGroup(rs);
                } else {
                    return new PostgresNodeGroup(groupId);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }
    }

    public List<PostgresNodeGroup> getNodeGroups(final Connection connection, final List<String> groupIds) throws SQLException {
        final List<PostgresNodeGroup> list = new ArrayList<>();
        for (final String group : groupIds) {
            list.add(getNodeGroup(connection, group));
        }
        return list;
    }

    public List<PostgresNodeGroup> getNodeGroups(final Connection connection) throws SQLException {
        List<PostgresNodeGroup> groups;
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_ALL_GROUPS)) {
            groups = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    groups.add(PostgresNodeGroup.createNodeGroup(rs));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }
        return groups;
    }
}
