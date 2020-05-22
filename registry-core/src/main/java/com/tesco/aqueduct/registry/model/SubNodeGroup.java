package com.tesco.aqueduct.registry.model;

import lombok.EqualsAndHashCode;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;

@EqualsAndHashCode
public class SubNodeGroup {

    private static final int NUMBER_OF_CHILDREN_PER_NODE = 2;

    public final List<Node> nodes;

    public final String subGroupId; //this is version for now

    public SubNodeGroup(String subGroupId) {
        this.nodes = new ArrayList<>();
        this.subGroupId = subGroupId;
    }

    public boolean isFor(Node node) {
        return node.getSubGroupId().equals(subGroupId);
    }

    public Node add(Node node, URL cloudUrl) {
        final List<URL> followUrls = calculateFollowerUrls(cloudUrl, getNodeUrls().size());
        final Node newNode = node.buildWith(followUrls);
        nodes.add(newNode);
        return newNode;
    }

    public Node add(Node newNode) {
        nodes.add(newNode);
        return newNode;
    }

    private List<URL> getNodeUrls() {
        return nodes.stream()
            .map(Node::getLocalUrl)
            .collect(Collectors.toList());
    }

    private List<URL> calculateFollowerUrls(final URL cloudUrl, int nodeIndex) {
        final List<URL> followUrls = new ArrayList<>();

        if (nodeIndex == 0) {
            followUrls.add(cloudUrl);
        } else {
            int parentNodeIndex = ((nodeIndex + 1) / NUMBER_OF_CHILDREN_PER_NODE) - 1;
            followUrls.add(nodes.get(parentNodeIndex).getLocalUrl());
            followUrls.addAll(nodes.get(parentNodeIndex).getRequestedToFollow());
        }

        return followUrls;
    }

    public void updateGetFollowing(URL cloudUrl) {
        IntStream.range(0, nodes.size())
            .forEach(i -> {
                final Node updatedNode = nodes
                    .get(i)
                    .toBuilder()
                    .requestedToFollow(calculateFollowerUrls(cloudUrl, i))
                    .build();

                this.updateNodeByIndex(updatedNode, i);
        });
    }

    private void updateNodeByIndex(final Node updatedNode, int index) {
        nodes.set(index, updatedNode);
    }

    public void markNodesOfflineIfNotSeenSince(ZonedDateTime threshold) {
        IntStream.range(0, nodes.size())
            .filter(i -> nodes.get(i).getLastSeen().compareTo(threshold) < 0)
            .forEach(i -> updateNodeByIndex(nodes.get(i).toBuilder().status(OFFLINE).build(), i));
    }

    public Node getById(String nodeId) {
        return nodes
            .stream()
            .filter(n -> n.getId().equals(nodeId))
            .findAny()
            .orElse(null);
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public boolean removeByHost(String host) {
        return nodes.removeIf(node -> node.getHost().equals(host));
    }

    public Node get(int index) {
        return nodes.get(index);
    }

    public Node update(Node updatedNode) throws IllegalStateException {
        return IntStream.range(0, nodes.size())
            .filter(i -> nodes.get(i).getId().equals(updatedNode.getId()))
            .mapToObj(i -> nodes.set(i, updatedNode))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("The node was not found " + updatedNode.getId()));
    }

    public void sortOfflineNodes(URL cloudUrl) {
        nodes.sort(this::comparingStatus);
        updateGetFollowing(cloudUrl);
    }

    private int comparingStatus(Node node1, Node node2) {
        if (node1.isOffline() && !node2.isOffline()) {
            return 1;
        } else if (!node1.isOffline() && node2.isOffline()) {
            return -1;
        } else {
            return 0;
        }
    }

    public Node upsert(final Node node, final URL cloudUrl) {
        return nodes.stream()
            .filter(n -> n.getId().equals(node.getId()))
            .findFirst()
            .map(n -> update(node.buildWith(n.getRequestedToFollow())))
            .orElseGet(() -> add(node, cloudUrl));


    }
}
