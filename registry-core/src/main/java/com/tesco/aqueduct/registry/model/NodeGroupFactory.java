package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.databind.JavaType;
import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NodeGroupFactory {
    private static final String QUERY_GET_GROUP_BY_ID = "SELECT entry, version FROM registry where group_id = ? ;";
    private static final String QUERY_GET_ALL_GROUPS = "SELECT entry, version FROM registry ORDER BY group_id";

    public static NodeGroup getNodeGroup(Connection connection, String groupId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_GROUP_BY_ID)) {
            statement.setString(1, groupId);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return createNodeGroup(rs);
                } else {
                    return new NodeGroup();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }
    }

    public static List<NodeGroup> getNodeGroups(Connection connection, List<String> groupIds) throws SQLException {
        List<NodeGroup> list = new ArrayList<>();
        for (String group : groupIds) {
            list.add(getNodeGroup(connection, group));
        }
        return list;
    }

    public static List<NodeGroup> getNodeGroups(Connection connection) throws SQLException {
        List<NodeGroup> groups;
        try (PreparedStatement statement = connection.prepareStatement(QUERY_GET_ALL_GROUPS)) {
            groups = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    groups.add(createNodeGroup(rs));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            }
        }
        return groups;
    }

    private static NodeGroup createNodeGroup(ResultSet rs) throws SQLException, IOException {
        String entry = rs.getString("entry");
        int version = rs.getInt("version");
        List<Node> nodes = readGroupEntry(entry);
        return new NodeGroup(nodes, version);
    }

    private static List<Node> readGroupEntry(String entry) throws IOException {
        JavaType type = JsonHelper.MAPPER.getTypeFactory().constructCollectionType(List.class, Node.class);
        return JsonHelper.MAPPER.readValue(entry, type);
    }
}
