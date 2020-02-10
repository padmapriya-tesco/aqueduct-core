package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.http.PipeStateProvider;

import java.util.List;

public class DistributedStorage implements MessageStorage {

    private final MessageReader<MessageEntity> messageReader;
    private final MessageWriter messageWriter;
    private final PipeStateProvider pipeStateProvider;

    public DistributedStorage(MessageReader<MessageEntity> messageReader, MessageWriter messageWriter, PipeStateProvider pipeStateProvider) {
        this.messageReader = messageReader;
        this.messageWriter = messageWriter;
        this.pipeStateProvider = pipeStateProvider;
    }

    @Override
    public MessageResults read(List<String> types, long offset, String locationUuid) {
        return new MessageResults(messageReader.read(types, offset, locationUuid), pipeState());
    }

    private PipeState pipeState() {
        return pipeStateProvider.getState();
    }

    @Override
    public long getLatestOffsetMatching(List<String> types) {
        return messageReader.getLatestOffsetMatching(types);
    }

    @Override
    public void write(Message message) {
        messageWriter.write(message);
    }

    @Override
    public void write(OffsetEntity offset) {
        messageWriter.write(offset);
    }

    @Override
    public void write(PipeState pipeState) {
        pipeStateProvider.setState(pipeState);
    }

    @Override
    public void deleteAll() {
        messageWriter.deleteAll();
    }
}
