package com.tesco.aqueduct.registry;

import lombok.Data;
import java.util.List;

@Data
public class NodeGroup {
    final List<Node> nodes;
    final int version;
}
