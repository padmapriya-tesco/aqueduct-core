package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;

import java.net.URL;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgreSQLNodeRegistry implements NodeRegistry {

    private final PostgresqlStorage postgresqlStorage;
    private final URL cloudUrl;
    private final Duration offlineDelta;
    private static final int NUMBER_OF_CHILDREN = 2;

    public PostgreSQLNodeRegistry(PostgresqlStorage postgresqlStorage, URL cloudUrl, Duration offlineDelta) {
        this.postgresqlStorage = postgresqlStorage;
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
    }

    @Override
    public List<URL> register(Node node) {
        List<Node> groupNodes = postgresqlStorage.getNodeGroup(node.getGroup());
        List<URL> nodeRequestedToFollow;

        ZonedDateTime now = ZonedDateTime.now();

        if(groupNodes.isEmpty()) {
            //empty so node is first of its group
            groupNodes = addNodeToNewGroup(node, now);
            nodeRequestedToFollow = node.getRequestedToFollow();
        } else {
            //not empty so group pre-exists
            Optional<Node> existingNode = groupNodes.stream()
                                    .filter(n -> n.getId().equals(node.getId()))
                                    .findAny();

            if(existingNode.isPresent()) {
                nodeRequestedToFollow = updateExistingNode(existingNode.get(), node, groupNodes);
            } else {
                nodeRequestedToFollow = addNodeToExistingGroup(groupNodes, node, now);
            }
        }

        postgresqlStorage.updateOrAddGroup(groupNodes);

        return nodeRequestedToFollow;
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groups) {

        Stream<Node> followers;

        if (groups == null || groups.isEmpty()) {
            followers = postgresqlStorage.getAllFollowers();
        } else {
            followers = getFollowersFilteredByGroups(groups);
        }

        followers = followers.map(this::changeStatusIfOffline);

        Node node = Node.builder()
                .localUrl(cloudUrl)
                .offset(offset)
                .status(status)
                .following(Collections.emptyList())
                .lastSeen(ZonedDateTime.now())
                .build();

        return new StateSummary(node, followers.collect(Collectors.toList()));
    }

    private Stream<Node> getFollowersFilteredByGroups(List<String> groups) {
        TreeMap<String, List<Node>> nodes =  postgresqlStorage.getNodes();

        return nodes.entrySet().stream()
                .filter(entry -> groups.contains(entry.getKey()))
                .flatMap(entry-> entry.getValue().stream());
    }

    private Node changeStatusIfOffline(Node node) {
        ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);

        if (node.getLastSeen().compareTo(threshold) < 0) {
            return node.toBuilder().status("offline").build();
        }
        return node;
    }

    private List<URL> updateExistingNode(Node existingValue, Node newValues, List<Node> groupNodes) {
        for (int i = 0; i < groupNodes.size(); i++) {
            if (groupNodes.get(i).getId().equals(newValues.getId())) {
                Node updatedNode = newValues.toBuilder()
                        .requestedToFollow(existingValue.getRequestedToFollow())
                        .lastSeen(ZonedDateTime.now())
                        .build();

                groupNodes.set(i, updatedNode);

                return updatedNode.getFollowing();
            }
        }

        throw new IllegalStateException("The node was not found " + newValues.getId());
    }

    private List<Node> addNodeToNewGroup(Node node, ZonedDateTime now) {
        List<Node> groupNodes = new ArrayList<>();
        List<URL> followUrls = Collections.singletonList(cloudUrl);

        Node updatedNode =
            node.toBuilder()
                .requestedToFollow(followUrls)
                .lastSeen(now)
                .build();

        groupNodes.add(updatedNode);

        return groupNodes;
    }

    private List<URL> addNodeToExistingGroup(List<Node> groupNodes, Node node, ZonedDateTime now) {
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

        return followUrls;
    }
}
