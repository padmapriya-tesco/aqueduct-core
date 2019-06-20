package com.tesco.aqueduct.registry;

import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgreSQLNodeRegistry implements NodeRegistry {

    private final URL cloudUrl;
    private final Duration offlineDelta;
    private final DataSource dataSource;
    private static final int NUMBER_OF_CHILDREN = 2;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRegistry.class));

    public PostgreSQLNodeRegistry(DataSource dataSource, URL cloudUrl, Duration offlineDelta) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.dataSource = dataSource;
    }

    @Override
    public List<URL> register(Node node) {
        try (Connection connection = dataSource.getConnection()) {
            List<Node> groupNodes = getNodeGroup(connection, node.getGroup());
            ZonedDateTime now = ZonedDateTime.now();

            if (groupNodes.isEmpty()) {
                return addNodeToNewGroup(connection, node, now);
            } else {
                //not empty so find if node is in group
                Optional<Node> existingNode = groupNodes
                    .stream()
                    .filter(n -> n.getId().equals(node.getId()))
                    .findAny();

                if (existingNode.isPresent()) {
                    return updateExistingNode(connection, existingNode.get(), node, groupNodes);
                } else {
                    return addNodeToExistingGroup(connection, groupNodes, node, now);
                }
            }
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "register node", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groups) {

        try (Connection connection = dataSource.getConnection()) {

            List<Node> followers;

            if (groups == null || groups.isEmpty()) {
                followers = getAllNodes();
            } else {
                followers = getNodesFilteredByGroup(connection, groups);
            }

            followers = followers
                .stream()
                .map(this::changeStatusIfOffline)
                .collect(Collectors.toList());

            Node node = Node.builder()
                    .localUrl(cloudUrl)
                    .offset(offset)
                    .status(status)
                    .following(Collections.emptyList())
                    .lastSeen(ZonedDateTime.now())
                    .build();

            return new StateSummary(node, followers);
        } catch (SQLException exception) {
            LOG.error("Postgresql node registry", "get summary", exception);
            throw new RuntimeException(exception);
        }
    }

    private List<URL> updateExistingNode(Connection connection, Node existingValue, Node newValues, List<Node> groupNodes) {
        for (int i = 0; i < groupNodes.size(); i++) {
            if (groupNodes.get(i).getId().equals(newValues.getId())) {
                Node updatedNode = newValues.toBuilder()
                    .requestedToFollow(existingValue.getRequestedToFollow())
                    .lastSeen(ZonedDateTime.now())
                    .build();

                groupNodes.set(i, updatedNode);
                persistGroup(connection, groupNodes);

                return updatedNode.getFollowing();
            }
        }

        throw new IllegalStateException("The node was not found " + newValues.getId());
    }

    private List<URL> addNodeToNewGroup(Connection connection, Node node, ZonedDateTime now) {
        List<Node> groupNodes = new ArrayList<>();
        List<URL> followUrls = Collections.singletonList(cloudUrl);

        Node updatedNode =
            node.toBuilder()
                .requestedToFollow(followUrls)
                .lastSeen(now)
                .build();

        groupNodes.add(updatedNode);

        persistGroup(connection, groupNodes);

        return followUrls;
    }

    private List<URL> addNodeToExistingGroup(Connection connection, List<Node> groupNodes, Node node, ZonedDateTime now) {
        List<URL> allUrls = groupNodes.stream().map(Node::getLocalUrl).collect(Collectors.toList());

        int nodeIndex = allUrls.size();
        List<URL> followUrls = new ArrayList<>();

        while (nodeIndex != 0) {
            nodeIndex = ((nodeIndex+1)/NUMBER_OF_CHILDREN) - 1;
            followUrls.add(allUrls.get(nodeIndex));
        }

        followUrls.add(cloudUrl);

        groupNodes.add(
            node.toBuilder()
                .requestedToFollow(followUrls)
                .lastSeen(now)
                .build()
            );

        persistGroup(connection, groupNodes);

        return followUrls;
    }

    private Node changeStatusIfOffline(Node node) {
        ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);

        if (node.getLastSeen().compareTo(threshold) < 0) {
            return node.toBuilder().status("offline").build();
        }
        return node;
    }

    //Methods that interact with the database - possibly need to split these into another class

    private List<Node> getNodeGroup(Connection connection, String group) {
        return Collections.emptyList();
    }

    private void persistGroup(Connection connection, List<Node> groupNodes) {
        //store or update group in the database
    }

    private List<Node> getAllNodes() {
        return Collections.emptyList();
    }

    private List<Node> getNodesFilteredByGroup(Connection connection, List<String> groups) {
        return groups
            .stream()
            .map(g -> getNodeGroup(connection, g))
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }
}
