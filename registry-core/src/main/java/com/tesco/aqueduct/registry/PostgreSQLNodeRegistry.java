package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeGroup;
import com.tesco.aqueduct.registry.model.NodeGroupFactory;
import com.tesco.aqueduct.registry.model.PostgresNodeGroup;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLNodeRegistry implements NodeRegistry {

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private static final int OPTIMISTIC_LOCKING_COOLDOWN = 5;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    public PostgreSQLNodeRegistry(DataSource dataSource, URL cloudUrl, Duration offlineDelta) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
    }

    @Override
    public List<URL> register(Node node) {
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                NodeGroup group = NodeGroupFactory.getNodeGroup(connection, node.getGroup());
                if (group.isEmpty()) {
                    node = group.add(node, cloudUrl);
                    insertNewGroup(connection, group);
                } else {
                    Node existingNode = group.getById(node.getId());
                    if (existingNode != null) {
                        node = updateExistingNode(existingNode, node, group);
                    } else {
                        node = group.add(node, cloudUrl);
                    }
                    updateGroup(connection, group);
                }
                return node.getRequestedToFollow();
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "register node", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groupIds) {
        try (Connection connection = dataSource.getConnection()) {
            List<PostgresNodeGroup> groups;

            if (groupIds == null || groupIds.isEmpty()) {
                groups = NodeGroupFactory.getNodeGroups(connection);
            } else {
                groups = NodeGroupFactory.getNodeGroups(connection, groupIds);
            }

            ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);
            List<Node> followers = groups.stream()
                .map(group -> group.markNodesOfflineIfNotSeenSince(threshold))
                .map(group -> group.nodes)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            return new StateSummary(getCloudNode(offset, status), followers);
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
    }

    private Node getCloudNode(long offset, String status) {
        return Node.builder()
            .localUrl(cloudUrl)
            .offset(offset)
            .status(status)
            .following(Collections.emptyList())
            .lastSeen(ZonedDateTime.now())
            .build();
    }

    @Override
    public boolean deleteNode(String groupId, String nodeId) {
        while (true) {
            try (Connection connection = dataSource.getConnection()) {
                PostgresNodeGroup nodeGroup = NodeGroupFactory.getNodeGroup(connection, groupId);

                if(nodeGroup.isEmpty()) {
                    return false;
                } else {
                    return deleteExistingNode(connection, nodeId, nodeGroup);
                }
            } catch (SQLException | IOException exception) {
                LOG.error("Postgresql node registry", "deleteNode", exception);
                throw new RuntimeException(exception);
            } catch (VersionChangedException exception) {
                try {
                    Thread.sleep(OPTIMISTIC_LOCKING_COOLDOWN);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean deleteExistingNode(Connection connection, String nodeId, PostgresNodeGroup group) throws IOException, SQLException {
        boolean foundNode = group.removeById(nodeId);

        if (foundNode) {
            if (group.isEmpty()) {
                deleteGroup(connection, group);
            } else {
                NodeGroup rebalancedGroup = group.rebalance(cloudUrl);
                updateGroup(connection, rebalancedGroup);
            }
            return true;
        }
        return false;
    }

    private void deleteGroup(Connection connection, PostgresNodeGroup group) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DELETE_GROUP)) {
            statement.setString(1, group.groupId);
            statement.setInt(2, group.version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }

    private Node updateExistingNode(Node existingValue, Node newValues, NodeGroup group) {
        //create a new node, with the existing "requestedToFollow" values
        Node updatedNode = newValues.toBuilder()
            .requestedToFollow(existingValue.getRequestedToFollow())
            .lastSeen(ZonedDateTime.now())
            .build();
        return group.updateNode(updatedNode);
    }

    private void updateGroup(Connection connection, NodeGroup group) throws SQLException, IOException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_GROUP)) {
            String jsonNodes = group.nodesToJson();

            statement.setString(1, jsonNodes);
            statement.setString(2, group.get(0).getGroup());
            statement.setInt(3, group.version);

            if (statement.executeUpdate() == 0) {
                throw new VersionChangedException();
            }
        }
    }

    private boolean insertNewGroup(Connection connection, NodeGroup group) throws IOException, SQLException {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_GROUP)) {
            String jsonNodes = group.nodesToJson();
            statement.setString(1, group.get(0).getGroup());
            statement.setString(2, jsonNodes);

            if (statement.executeUpdate() == 0) {
                //No rows updated
                throw new VersionChangedException();
            }
            return true;
        }
    }

    private static final String QUERY_UPDATE_GROUP =
        "UPDATE registry SET " +
            "entry = ?::JSON , " +
            "version = registry.version + 1 " +
        "WHERE " +
            "registry.group_id = ? " +
        "AND " +
            "registry.version = ? " +
        ";";

    private static final String QUERY_INSERT_GROUP =
        "INSERT INTO registry (group_id, entry, version)" +
        "VALUES (" +
            "?, " +
            "?::JSON, " +
            "0 " +
        ")" +
        "ON CONFLICT DO NOTHING ;";

    private static final String QUERY_DELETE_GROUP = "DELETE from registry where group_id = ? and version = ? ;";
}
