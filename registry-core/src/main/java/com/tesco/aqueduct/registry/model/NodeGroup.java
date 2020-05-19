package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;

public class NodeGroup {
    public final List<Node> nodes;

    public final List<SubNodeGroup> subGroups ;

    public NodeGroup() {
        this(new ArrayList<>());
    }

    public NodeGroup(final List<Node> nodes) {
        this.nodes = nodes;
        subGroups = new ArrayList<>();
        SubNodeGroup subNodeGroup = new SubNodeGroup(nodes);
        subGroups.add(subNodeGroup);
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public boolean removeByHost(final String host) {
        return nodes.removeIf(node -> node.getHost().equals(host));
    }

    public Node add(final Node node, final URL cloudUrl) {
        return subGroups.get(0).add(node, cloudUrl);
    }

    public Node get(final int index) {
        return nodes.get(index);
    }

    public Node getById(final String nodeId) {
        return subGroups.get(0).getById(nodeId);
    }

    public Node updateNode(final Node updatedNode) {
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

    public void updateGetFollowing(final URL cloudUrl) {
        subGroups.get(0).updateGetFollowing(cloudUrl);
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        subGroups.get(0).markNodesOfflineIfNotSeenSince(threshold);
    }

    public void sortOfflineNodes(final URL cloudUrl) {
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


    public Node upsert(Node nodeToRegister, URL cloudUrl) {

        Node node = getById(nodeToRegister.getId());

        if (node == null) {
            node = add(nodeToRegister, cloudUrl);
        } else {
            node = nodeToRegister.buildWith(node.getRequestedToFollow(), ZonedDateTime.now());
            updateNode(node);
        }

        return node;
    }

}