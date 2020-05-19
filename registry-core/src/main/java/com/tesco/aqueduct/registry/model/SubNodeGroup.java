package com.tesco.aqueduct.registry.model;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;

public class SubNodeGroup {

    private static final int NUMBER_OF_CHILDREN_PER_NODE = 2;

    public final List<Node> nodes;

    public final String subGroupId; //this is version for now

    public SubNodeGroup(List<Node> nodes, String subGroupId) {
        this.nodes = nodes;
        this.subGroupId = subGroupId;
    }

    public boolean isNodeMember(Node node) {
        return node.getPipeVersion().equals(subGroupId);
    }

    public Node add(Node node, URL cloudUrl) {
        final List<URL> followUrls = calculateFollowerUrls(cloudUrl, getNodeUrls().size());
        final Node newNode = node.buildWith(followUrls);
        nodes.add(newNode);
        return newNode;
    }

    public List<URL> getNodeUrls() {
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
        for (int i = 0; i < nodes.size(); i++) {
            final List<URL> followUrls = calculateFollowerUrls(cloudUrl, i);
            final Node updatedNode = nodes
                .get(i)
                .toBuilder()
                .requestedToFollow(followUrls)
                .build();

            this.updateNodeByIndex(updatedNode, i);
        }
    }

    private void updateNodeByIndex(final Node updatedNode, int index) {
        nodes.set(index, updatedNode);
    }

    public void markNodesOfflineIfNotSeenSince(ZonedDateTime threshold) {
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            if (node.getLastSeen().compareTo(threshold) < 0) {
                updateNodeByIndex(node.toBuilder().status(OFFLINE).build(), i);
            }
        }
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

    public Node updateNode(Node updatedNode) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getId().equals(updatedNode.getId())) {
                return nodes.set(i, updatedNode);
            }
        }

        throw new IllegalStateException("The node was not found " + updatedNode.getId());
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

    public void addToList(Node node) {
        nodes.add(node);
    }
}
