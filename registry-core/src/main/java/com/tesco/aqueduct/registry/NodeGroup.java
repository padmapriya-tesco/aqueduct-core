package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import java.util.List;

public class NodeGroup {
    final List<Node> nodes;
    final int version;

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
}
