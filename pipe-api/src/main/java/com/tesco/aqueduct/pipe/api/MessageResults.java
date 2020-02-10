package com.tesco.aqueduct.pipe.api;

import java.util.List;
import java.util.OptionalLong;

public class MessageResults {
    private final MessageEntity messageEntity;
    private final PipeState pipeState;

    public MessageResults(MessageEntity messageEntity, PipeState pipeState) {
        this.messageEntity = messageEntity;
        this.pipeState = pipeState;
    }

    public List<Message> getMessages() {
        return messageEntity.getMessages();
    }

    public long getRetryAfterSeconds() {
        return messageEntity.getRetryAfterSeconds();
    }

    public OptionalLong getGlobalLatestOffset() {
        return messageEntity.getGlobalLatestOffset();
    }

    public PipeState getPipeState() {
        return pipeState;
    }
}
