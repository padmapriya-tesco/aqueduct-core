package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Node;
import lombok.Data;
import java.util.List;

@Data
public class StateSummary {
    private final Node root;
    private final List<Node> followers;
}
