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
        nodes.forEach(this::updateExistingOrAddNewSubNodeGroupFor);
    }

    private void updateExistingOrAddNewSubNodeGroupFor(Node node) {
        subGroups.stream()
            .filter(subNodeGroup -> subNodeGroup.isFor(node))
            .findFirst()
            .map(subNodeGroup -> {
                subNodeGroup.add(node);
                return node;
            })
            .orElseGet(() -> newSubGroupNodeFor(node));
    }

    private Node newSubGroupNodeFor(Node node) {
        SubNodeGroup subNodeGroup = new SubNodeGroup(node.getSubGroupId());
        subGroups.add(subNodeGroup);
        return subNodeGroup.add(node);
    }

    public boolean isEmpty() {
        return subGroups.isEmpty();
    }

    public boolean removeByHost(final String host) {
        boolean result = subGroups.stream().anyMatch(subgroup -> subgroup.removeByHost(host));
        subGroups.removeIf(SubNodeGroup::isEmpty);
        return result;
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(getNodes());
    }

    public List<Node> getNodes() {
        return subGroups.stream()
            .flatMap(subNodeGroup -> subNodeGroup.nodes.stream()).collect(Collectors.toList());
    }

    public void updateGetFollowing(final URL cloudUrl) {
        subGroups.forEach(subgroup -> subgroup.updateGetFollowing(cloudUrl));
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        subGroups.forEach(subGroup -> subGroup.markNodesOfflineIfNotSeenSince(threshold));
    }

    public void sortOfflineNodes(final URL cloudUrl) {
        subGroups.forEach(subGroup -> subGroup.sortOfflineNodes(cloudUrl));
    }

    public Node upsert(final Node nodeToRegister, final URL cloudUrl) {
        SubNodeGroup subGroup = findOrCreateSubGroup(nodeToRegister);

        return subGroup.getByHost(nodeToRegister.getHost())
            .map(existingNode -> subGroup.update(existingNode, nodeToRegister))
            .orElseGet(() -> {
                Node newNode = subGroup.add(nodeToRegister, cloudUrl);
                removeNodeIfSwitchingSubgroup(nodeToRegister);
                return newNode;
            });
    }

    private void removeNodeIfSwitchingSubgroup(final Node nodeToRegister) {
        subGroups.stream()
            .filter(subNodeGroup -> subNodeGroup.getByHost(nodeToRegister.getHost())
                .filter(node -> node.isSubGroupIdDifferent(nodeToRegister))
                .map(node -> subNodeGroup.removeByHost(nodeToRegister.getHost()))
                .orElse(false))
            .findFirst()
            .ifPresent(this::removeSubGroupIfEmpty);
    }

    private SubNodeGroup findOrCreateSubGroup(Node nodeToRegister) {
        return subGroups.stream()
            .filter(subGroup -> subGroup.isFor(nodeToRegister))
            .findFirst()
            .orElseGet(() -> {
                SubNodeGroup subNodeGroup = new SubNodeGroup(nodeToRegister.getSubGroupId());
                subGroups.add(subNodeGroup);
                return subNodeGroup;
            });
    }

    private void removeSubGroupIfEmpty(SubNodeGroup subNodeGroup) {
        if(subNodeGroup.isEmpty()) {
            subGroups.remove(subNodeGroup);
        }
    }
}