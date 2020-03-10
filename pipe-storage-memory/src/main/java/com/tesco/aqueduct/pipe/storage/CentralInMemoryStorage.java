package com.tesco.aqueduct.pipe.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tesco.aqueduct.pipe.api.*;
import lombok.Getter;
import java.util.*;

public class CentralInMemoryStorage extends InMemoryStorage implements CentralStorage {

    public CentralInMemoryStorage(int limit, long retryAfter) {
        super(limit, retryAfter);
    }

    @Override
    protected OptionalLong getLatestGlobalOffset() {
        return messages.stream().mapToLong(Message::getOffset).max();
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return messages.stream().mapToLong(Message::getOffset).max();
    }

    public void write(Message message, String clusterId) {
        write(new ClusteredMessage(message, clusterId));
    }

    @Override
    boolean messageMatchCluster(Message message, List<String> clusterIds) {
        ClusteredMessage clusteredMessage = (ClusteredMessage) message;
        return (clusterIds == null || clusterIds.isEmpty()) || clusterIds.contains(clusteredMessage.getClusterId());
    }

    @Getter
    public static class ClusteredMessage extends Message {
        @JsonIgnore
        private final String clusterId;

        public ClusteredMessage(Message message, String clusterId) {
            super(message.getType(), message.getKey(), message.getContentType(), message.getOffset(), message.getCreated(), message.getData());
            this.clusterId = clusterId;
        }

        @Override
        public Message withOffset(Long offset) {
            return new ClusteredMessage(super.withOffset(offset), clusterId);
        }
    }
}
