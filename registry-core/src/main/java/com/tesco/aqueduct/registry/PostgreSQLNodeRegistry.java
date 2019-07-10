package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.PostgresNodeGroup;
import com.tesco.aqueduct.registry.model.StateSummary;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
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
                PostgresNodeGroup group = PostgresNodeGroup.getNodeGroup(connection, node.getGroup());
                if (group.isEmpty()) {
                    node = group.add(node, cloudUrl);
                } else {
                    Node existingNode = group.getById(node.getId());
                    if (existingNode != null) {
                        node = updateExistingNode(existingNode, node);
                        group.updateNode(node);
                    } else {
                        node = group.add(node, cloudUrl);
                    }
                }
                group.persist(connection);
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
                groups = PostgresNodeGroup.getNodeGroups(connection);
            } else {
                groups = PostgresNodeGroup.getNodeGroups(connection, groupIds);
            }

            ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);
            groups.forEach(group -> group.markNodesOfflineIfNotSeenSince(threshold));

            List<Node> followers = groups.stream()
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
                PostgresNodeGroup nodeGroup = PostgresNodeGroup.getNodeGroup(connection, groupId);

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
                group.delete(connection);
            } else {
                group.rebalance(cloudUrl);
                group.persist(connection);
            }
            return true;
        }
        return false;
    }

    private Node updateExistingNode(Node existingValue, Node newValues) {
        return newValues.toBuilder()
            .requestedToFollow(existingValue.getRequestedToFollow())
            .lastSeen(ZonedDateTime.now())
            .build();
    }
}
