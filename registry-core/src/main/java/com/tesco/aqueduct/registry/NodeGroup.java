package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.registry.model.Node;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NodeGroup {
    private static final int UNPERSISTED_GROUP_VERSION = Integer.MIN_VALUE;
    private static final int NUMBER_OF_CHILDREN_PER_NODE = 2;

    final List<Node> nodes;
    final int version;

    public NodeGroup(Node... nodes) {
        this(Arrays.asList(nodes), UNPERSISTED_GROUP_VERSION);
    }

    public NodeGroup(List<Node> nodes, int version) {
        this.nodes = nodes;
        this.version = version;
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public boolean removeById(String nodeId) {
        return nodes.removeIf(node -> node.getId().equals(nodeId));
    }

    public Node add(Node node, URL cloudUrl) {
        List<URL> followUrls = getFollowerUrls(cloudUrl);

        Node newNode = node.toBuilder()
            .requestedToFollow(followUrls)
            .lastSeen(ZonedDateTime.now())
            .build();

        nodes.add(newNode);

        return newNode;
    }

    public Node get(int index) {
        return nodes.get(index);
    }

    public Node getById(String nodeId) {
        return nodes
            .stream()
            .filter(n -> n.getId().equals(nodeId))
            .findAny()
            .orElse(null);
    }

    public List<URL> getNodeUrls() {
        return nodes.stream()
            .map(Node::getLocalUrl)
            .collect(Collectors.toList());
    }

    public Node updateNode(Node updatedNode) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getId().equals(updatedNode.getId())) {
                return nodes.set(i, updatedNode);
            }
        }

        throw new IllegalStateException("The node was not found " + updatedNode.getId());
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(nodes);
    }

    public NodeGroup rebalance(URL cloudUrl) {
        List<URL> allUrls = getNodeUrls();
        List<Node> rebalancedNodes = new ArrayList<>();

        for (int i = 0; i < allUrls.size(); i++) {
            List<URL> followUrls = getFollowerUrls(cloudUrl, i);

            Node updatedNode = nodes
                .get(i)
                .toBuilder()
                .requestedToFollow(followUrls)
                .build();

            rebalancedNodes.add(updatedNode);
        }
        return new NodeGroup(rebalancedNodes, version);
    }

    private List<URL> getFollowerUrls(URL cloudUrl) {
        return getFollowerUrls(cloudUrl, getNodeUrls().size());
    }

    private List<URL> getFollowerUrls(URL cloudUrl, int nodeIndex) {
        List<URL> followUrls = new ArrayList<>();

        List<URL> allUrls = getNodeUrls();
        if (nodeIndex < 0) nodeIndex = allUrls.size();
        while (nodeIndex != 0) {
            nodeIndex = ((nodeIndex + 1) / NUMBER_OF_CHILDREN_PER_NODE) - 1;
            followUrls.add(allUrls.get(nodeIndex));
        }

        followUrls.add(cloudUrl);
        return followUrls;
    }

    public NodeGroup markNodesOfflineIfNotSeenSince(ZonedDateTime threshold) {
        List<Node> updatedNodes = new ArrayList<>();
        for (Node node : nodes) {
            if (node.getLastSeen().compareTo(threshold) < 0) {
                updatedNodes.add(node.toBuilder().status("offline").build());
            } else {
                updatedNodes.add(node);
            }
        }
        return new NodeGroup(updatedNodes, version);
    }
}
