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

        removeNodeIfSwitchingSubgroup(nodeToRegister);

        return subGroups.stream()
            .filter(subGroup -> subGroup.isFor(nodeToRegister))
            .findFirst()
            .map(subGroup -> subGroup.upsert(nodeToRegister, cloudUrl))
            .orElseGet(() -> {
                SubNodeGroup subNodeGroup = new SubNodeGroup(nodeToRegister.getSubGroupId());
                subGroups.add(subNodeGroup);
                return subNodeGroup.add(nodeToRegister, cloudUrl);
            });
    }

    private void removeNodeIfSwitchingSubgroup(final Node nodeToRegister) {
        subGroups.stream()
        .filter(subgroup -> subgroup.getByHost(nodeToRegister.getHost()) != null)
        .findFirst()
        .ifPresent(subgroup -> {
            Node node = subgroup.getByHost(nodeToRegister.getHost());

            if (!node.getSubGroupId().equals(nodeToRegister.getSubGroupId())) {
                subgroup.removeByHost(nodeToRegister.getHost());
                subGroups.removeIf(SubNodeGroup::isEmpty);
            }
        });
    }
}