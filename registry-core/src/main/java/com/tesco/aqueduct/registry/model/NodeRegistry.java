package com.tesco.aqueduct.registry.model;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;

public interface NodeRegistry {
    /**
     * @param node Node to register as new or update current state
     * @return List of URL this node should currently follow
     */
    List<URL> register(Node node);

    NodeGroup getNodeGroup(Connection connection, String groupId) throws IOException, SQLException;

    Node upsert(Node node, URL cloudUrl, NodeGroup nodeGroup);

    void processOfflineNodes(NodeGroup nodeGroup, ZonedDateTime threshold, URL cloudUrl);

    /**
     * @param offset Latest offset of root
     * @param status Status of root
     * @param groups List of groups to return, all if empty or null
     * @return Summary of all currently known nodes
     */
    StateSummary getSummary(long offset, Status status, List<String> groups);

    boolean deleteNode(String group, String host);
}
