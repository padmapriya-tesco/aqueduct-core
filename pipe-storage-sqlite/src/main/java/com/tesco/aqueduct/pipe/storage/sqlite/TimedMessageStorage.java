package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.MessageStorage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.List;

public class TimedMessageStorage implements MessageStorage {
    private final MessageStorage storage;
    private final Timer readTimer;
    private final Timer latestOffsetTimer;
    private final Timer writeMessageTimer;
    private final Timer writeMessagesTimer;

    public TimedMessageStorage(final MessageStorage storage, final MeterRegistry meterRegistry) {
        this.storage = storage;
        readTimer = meterRegistry.timer("pipe.storage.read");
        latestOffsetTimer = meterRegistry.timer("pipe.storage.latestOffset");
        writeMessageTimer = meterRegistry.timer("pipe.storage.writeMessage");
        writeMessagesTimer = meterRegistry.timer("pipe.storage.writeMessages");
    }

    @Override
    public MessageResults read(final List<String> types, final long offset, final String locationUuid) {
        return readTimer.record(() -> storage.read(types, offset, locationUuid));
    }

    @Override
    public long getLatestOffsetMatching(final List<String> types) {
        return latestOffsetTimer.record(() -> storage.getLatestOffsetMatching(types));
    }

    @Override
    public void write(final Iterable<Message> messages) {
        writeMessageTimer.record(() -> storage.write(messages));
    }

    @Override
    public void write(final Message message) {
        writeMessagesTimer.record(() -> storage.write(message));
    }

    @Override
    public void deleteAllMessages() {
        storage.deleteAllMessages();
    }
}
