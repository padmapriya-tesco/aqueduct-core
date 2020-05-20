package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NodeGroup {
    public final List<SubNodeGroup> subGroups = new ArrayList<>();

    public NodeGroup() {
        this(new ArrayList<>());
    }

    public NodeGroup(final List<Node> nodes) {
        nodes.forEach(node ->
            updateExistingOrAddNewSubNodeGroupFor(node)
        );
    }

    private Node updateExistingOrAddNewSubNodeGroupFor(Node node) {
        return subGroups.stream()
            .filter(subNodeGroup -> subNodeGroup.isFor(node))
            .findFirst()
            .map(subNodeGroup -> {
                subNodeGroup.add(node);
                return node;
            })
            .orElseGet(() -> newSubGroupNodeFor(node));
    }

    private Node newSubGroupNodeFor(Node node) {
        SubNodeGroup subNodeGroup = new SubNodeGroup(node.getPipeVersion());
        subGroups.add(subNodeGroup);
        return subNodeGroup.add(node);
    }

    public boolean isEmpty() {
        return subGroups.get(0).isEmpty();
    }

    public boolean removeByHost(final String host) {
        return subGroups.get(0).removeByHost(host);
    }

    public Node add(final Node node, final URL cloudUrl) {
//        return subGroups.get(0).add(node, cloudUrl);
        return null;
    }

    public Node get(final int index) {
        return subGroups.get(0).get(index);
    }

    public Node getById(final Node node) {
//        return subGroups.stream().filter(s -> s.isFor(node)).
//        return subGroups.get(0).getById(nodeId);
        return null;
    }

    public Node updateNode(final Node updatedNode) {
        return subGroups.get(0).update(updatedNode);
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(subGroups.stream().flatMap(subNodeGroup -> subNodeGroup.nodes.stream()).collect(Collectors.toList()));
    }

    public void updateGetFollowing(final URL cloudUrl) {
        subGroups.get(0).updateGetFollowing(cloudUrl);
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        subGroups.forEach(subGroup -> subGroup.markNodesOfflineIfNotSeenSince(threshold));
    }

    public void sortOfflineNodes(final URL cloudUrl) {
        subGroups.get(0).sortOfflineNodes(cloudUrl);
    }

    public Node upsert(final Node nodeToRegister, final URL cloudUrl) {
        return subGroups.stream()
            .filter(subGroup -> subGroup.isFor(nodeToRegister))
            .findFirst()
            .map(subGroup -> subGroup.upsert(nodeToRegister, cloudUrl))
            .orElseGet(() -> {
                SubNodeGroup subNodeGroup = new SubNodeGroup(nodeToRegister.getPipeVersion());
                subGroups.add(subNodeGroup);
                return subNodeGroup.add(nodeToRegister, cloudUrl);
            });
    }
}