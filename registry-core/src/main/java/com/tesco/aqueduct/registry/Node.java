package com.tesco.aqueduct.registry;

import lombok.Builder;
import lombok.Data;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.List;

@Builder(toBuilder = true)
@Data
public class Node {
    /**
     * Name of the group, usually store number 
     */
    private final String group;

    /**
     * Self URL resolvable by peers in the same group
     */
    private final URL localUrl;

    /**
     * The offset as last reported by this node
     */
    private final long offset;

    /**
     * Status as last reported by this node (computed status might be different)
     */
    private final String status;

    /**
     * List of nodes in order that current node will try to connect to
     */
    private final List<URL> following;

    /**
     * List of nodes in order that current was last told to follow.
     * Populated on server side.
     */
    private final List<URL> requestedToFollow;

    /**
     * Populated on server side as NOW
     */
    private final ZonedDateTime lastSeen;

    /**
     * The time at which the latest offset was received.
     */
    private final ZonedDateTime latestArrivalTime;

    /**
     * The last ACKed offset from the Provider
     */
    private final long providerLastAckOffset;

    /**
     * Tracks the time that the last message was ACKed by a client.
     */
    private final ZonedDateTime providerLastAckTime;

    public String getId() {
        if (group == null) {
            return localUrl.toString();
        } else {
            return String.format("%s|%s", group, localUrl.toString());
        }
    }
}
