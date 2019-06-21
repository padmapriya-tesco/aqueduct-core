package com.tesco.aqueduct.registry;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.time.Duration;
import java.time.ZoneId;
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
        } catch (SQLException | IOException exception) {
            LOG.error("Postgresql node registry", "register node", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groups) {

        try (Connection connection = dataSource.getConnection()) {

            List<Node> followers;

            if (groups == null || groups.isEmpty()) {
                followers = getAllNodes(connection);
            } else {
                followers = getNodesFilteredByGroup(connection, groups);
            }

            followers = followers
                .stream()
                .map(this::changeStatusIfOffline)
                .collect(Collectors.toList());

            //TODO: should this update the registry if a node is offline?

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

    private List<URL> updateExistingNode(Connection connection, Node existingValue, Node newValues, List<Node> groupNodes) throws SQLException, IOException {
        for (int i = 0; i < groupNodes.size(); i++) {
            if (groupNodes.get(i).getId().equals(newValues.getId())) {
                Node updatedNode = newValues.toBuilder()
                    .requestedToFollow(existingValue.getRequestedToFollow())
                    .lastSeen(ZonedDateTime.now())
                    .build();

                groupNodes.set(i, updatedNode);
                persistGroup(connection, groupNodes);

                return updatedNode.getRequestedToFollow();
            }
        }

        throw new IllegalStateException("The node was not found " + newValues.getId());
    }

    private List<URL> addNodeToNewGroup(Connection connection, Node node, ZonedDateTime now) throws SQLException, IOException {
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

    private List<URL> addNodeToExistingGroup(Connection connection, List<Node> groupNodes, Node node, ZonedDateTime now) throws SQLException, IOException {
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

    private List<Node> getNodesFilteredByGroup(Connection connection, List<String> groups) throws SQLException {
        List<Node> list = new ArrayList<>();
        for (String group : groups) {
            List<Node> nodeGroup = getNodeGroup(connection, group);
            list.addAll(nodeGroup);
        }
        return list;
    }

    // Methods that interact with the database - possibly need to split these into another class

    private List<Node> getNodeGroup(Connection connection, String group) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(getNodeGroupQuery());

        statement.setString(1, group);

        List<Node> nodes = new ArrayList<>();

        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String entry = rs.getString("entry");
                nodes.addAll(readGroupEntry(entry));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return nodes;
    }

    private List<Node> readGroupEntry(String entry) throws IOException {
        JavaType type = JsonHelper.MAPPER.getTypeFactory().constructCollectionType(List.class, Node.class);
        return JsonHelper.MAPPER.readValue(entry, type);
    }

    private void persistGroup(Connection connection, List<Node> groupNodes) throws SQLException, IOException {
        //store or update group in the database
        PreparedStatement statement = connection.prepareStatement(getPersistGroupQuery());

        String jsonNodes = JsonHelper.toJson(groupNodes);

        statement.setString(1, groupNodes.get(0).getGroup());
        statement.setString(2, jsonNodes);
        statement.setString(3, jsonNodes);

        statement.executeUpdate();
    }

    private List<Node> getAllNodes(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(getAllNodesQuery());

        List<Node> nodes = new ArrayList<>();

        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String entry = rs.getString("entry");
                nodes.addAll(readGroupEntry(entry));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return nodes;
    }

    private static String getNodeGroupQuery() {
        return "SELECT entry FROM registry where group_id = ? ;";
    }

    private static String getPersistGroupQuery() {
        return "INSERT INTO registry (group_id, entry) " +
                "VALUES ( " +
                "?," +
                "?::JSON" +
                ")" +
                "ON CONFLICT (group_id) " +
                "DO UPDATE SET " +
                "entry = ?::JSON ;";
    }

    private static String getAllNodesQuery() {
        return "SELECT entry FROM registry ;";
    }
}
