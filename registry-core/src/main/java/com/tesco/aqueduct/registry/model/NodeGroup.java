package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NodeGroup {
    public final List<SubNodeGroup> subGroups = new ArrayList<>();

    public NodeGroup() {
        this(new ArrayList<>());
    }

    public NodeGroup(final List<Node> nodes) {
        for(Node node : nodes) {
            subGroups.add(subGroups.stream()
                .filter(subNodeGroup -> subNodeGroup.isNodeMember(node))
                .findFirst()
                .map(subNodeGroup -> {
                    subNodeGroup.addToList(node);
                    return subNodeGroup;
                })
                .orElse(
                    new SubNodeGroup(Collections.singletonList(node), node.getPipeVersion())
                ));
        }
    }

    public boolean isEmpty() {
        return subGroups.get(0).isEmpty();
    }

    public boolean removeByHost(final String host) {
        return subGroups.get(0).removeByHost(host);
    }

    public Node add(final Node node, final URL cloudUrl) {
        subGroups.stream().filter(subNodeGroup -> subNodeGroup.isNodeMember(node))
            .findFirst()
            .ifPresen
        return subGroups.get(0).add(node, cloudUrl);
    }

    public Node get(final int index) {
        return subGroups.get(0).get(index);
    }

    public Node getById(final Node node) {
        return subGroups.stream().filter(s -> s.isNodeMember(node)).
        return subGroups.get(0).getById(nodeId);
    }

    public Node updateNode(final Node updatedNode) {
        return subGroups.get(0).updateNode(updatedNode);
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(subGroups.get(0).nodes);
    }

    public void updateGetFollowing(final URL cloudUrl) {
        subGroups.get(0).updateGetFollowing(cloudUrl);
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        subGroups.get(0).markNodesOfflineIfNotSeenSince(threshold);
    }

    public void sortOfflineNodes(final URL cloudUrl) {
        subGroups.get(0).sortOfflineNodes(cloudUrl);
    }

    public Node upsert(Node nodeToRegister, URL cloudUrl) {

        subGroups.stream()
            .filter(s -> s.isNodeMember(nodeToRegister))
            .findFirst()
            .map(s -> s.getById(nodeToRegister.getId()))
            .ifPresent();



        if (node == null) {
            node = add(nodeToRegister, cloudUrl);
        } else {
            node = nodeToRegister.buildWith(node.getRequestedToFollow());
            updateNode(node);
        }

        return node;
    }

}