package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.MessageStorage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.List;
import java.util.Map;

public class TimedMessageStorage implements MessageStorage {
    private final MessageStorage storage;
    private final Timer readTimer;
    private final Timer latestOffsetTimer;
    private final Timer writeMessageTimer;
    private final Timer writeMessagesTimer;

    public TimedMessageStorage(MessageStorage storage, MeterRegistry meterRegistry) {
        this.storage = storage;
        readTimer = meterRegistry.timer("pipe.storage.read");
        latestOffsetTimer = meterRegistry.timer("pipe.storage.latestOffset");
        writeMessageTimer = meterRegistry.timer("pipe.storage.writeMessage");
        writeMessagesTimer = meterRegistry.timer("pipe.storage.writeMessages");
    }

    @Override
    public MessageResults read(List<String> types, long offset) {
        return readTimer.record(() -> storage.read(types, offset));
    }

    @Override
    public long getLatestOffsetMatching(List<String> types) {
        return latestOffsetTimer.record(() -> storage.getLatestOffsetMatching(types));
    }

    @Override
    public void write(Iterable<Message> messages) {
        writeMessageTimer.record(() -> storage.write(messages));
    }

    @Override
    public void write(Message message) {
        writeMessagesTimer.record(() -> storage.write(message));
    }
}
