package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import lombok.Data;
import java.util.List;

@Data
public class NodeGroup {
    final List<Node> nodes;
    final int version;

    public boolean isEmpty() {
        return nodes.isEmpty();
    }
}
