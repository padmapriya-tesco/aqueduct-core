package com.tesco.aqueduct.registry;

import java.net.URL;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public class InMemoryNodeRegistry implements NodeRegistry {

    private final TreeMap<String, List<Node>> nodes;
    private final URL cloudUrl;
    private final Duration offlineDelta;
    private static final int NUMBER_OF_CHILDREN = 2;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    public InMemoryNodeRegistry(URL cloudUrl, Duration offlineDelta) {
        this.cloudUrl = cloudUrl;
        this.offlineDelta = offlineDelta;
        this.nodes = new TreeMap<>();
    }

    @Override
    public List<URL> register(Node node) {
        ReentrantReadWriteLock.WriteLock lock = rwl.writeLock();

        try {
            lock.lock();

            ZonedDateTime now = ZonedDateTime.now();
            List<Node> groupNodes = nodes.getOrDefault(node.getGroup(), emptyList());

            return groupNodes.stream()
                .filter(n -> n.getId().equals(node.getId()))
                .findAny()
                .map(value -> updateExistingNode(value, node, groupNodes).getRequestedToFollow())
                .orElseGet(() -> addNodeToGroup(node, now, groupNodes));
        } finally {
            lock.unlock();
        }
    }

    private List<URL> addNodeToGroup(Node node, ZonedDateTime now, List<Node> groupNodes) {
        if (groupNodes.isEmpty()) {
            return addNodeToNewGroup(node, now);
        } else {
            return addNodeToExistingGroup(groupNodes, node, now);
        }
    }

    private List<URL> addNodeToNewGroup(Node node, ZonedDateTime now) {
        List<Node> groupNodes = new ArrayList<>();
        List<URL> followUrls = Collections.singletonList(cloudUrl);

        Node updatedNode =
            node
                .toBuilder()
                .requestedToFollow(followUrls)
                .lastSeen(now)
                .build();

        groupNodes.add(updatedNode);

        nodes.put(node.getGroup(), groupNodes);

        return followUrls;
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

        nodes.get(node.getGroup())
            .add(
                node.toBuilder()
                    .requestedToFollow(followUrls)
                    .lastSeen(now)
                    .build()
            );

        return followUrls;
    }

    private Node updateExistingNode(Node existingValue, Node newValues, List<Node> groupNodes) {
        for (int i = 0; i < groupNodes.size(); i++) {
            if (groupNodes.get(i).getId().equals(newValues.getId())) {
                Node updatedNode = newValues.toBuilder()
                    .requestedToFollow(existingValue.getRequestedToFollow())
                    .lastSeen(ZonedDateTime.now())
                    .build();

                groupNodes.set(i, updatedNode);

                return updatedNode;
            }
        }

        throw new IllegalStateException("The node was not found " + newValues.getId());
    }

    @Override
    public StateSummary getSummary(long offset, String status, List<String> groups) {
        ReentrantReadWriteLock.ReadLock lock = rwl.readLock();

        try {
            lock.lock();

            Stream<Node> followers;

            if (groups == null || groups.isEmpty()) {
                followers = getAllFollowers();
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
        } finally {
            lock.unlock();
        }
    }

    private Node changeStatusIfOffline(Node node) {
        ZonedDateTime threshold = ZonedDateTime.now().minus(offlineDelta);

        if (node.getLastSeen().compareTo(threshold) < 0) {
            return node.toBuilder().status("offline").build();
        }
        return node;
    }

    private Stream<Node> getFollowersFilteredByGroups(List<String> groups) {
        return nodes.entrySet().stream()
            .filter(entry -> groups.contains(entry.getKey()))
            .flatMap(entry-> entry.getValue().stream());
    }

    private Stream<Node> getAllFollowers() {
        return nodes.values().stream().flatMap(List::stream);
    }
}
