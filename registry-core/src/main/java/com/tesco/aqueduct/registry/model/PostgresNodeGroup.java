package com.tesco.aqueduct.registry.model;

import java.util.List;

public class PostgresNodeGroup extends NodeGroup {
    public String groupId;

    PostgresNodeGroup() {
        super();
    }

    PostgresNodeGroup(String groupId, int version, List<Node> nodes) {
        super(nodes, version);
        this.groupId = groupId;
    }
}
