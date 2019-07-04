package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

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

    public void add(Node node) {
        this.nodes.add(node);
    }

    public Node get(int index) {
        return this.nodes.get(index);
    }

    public Node getById(String nodeId){
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
}
