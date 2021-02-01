package com.tesco.aqueduct.registry.model;

import java.util.List;

public interface NodeRegistry {

    Node register(Node node);

    StateSummary getSummary(long offset, Status status, List<String> groups);

    boolean deleteNode(String group, String host);
}
