package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.OptionalLong;

public class MetricizedDistributedStorage implements DistributedStorage {

    private final TimedDistributedStorage timedDistributedStorage;

    public MetricizedDistributedStorage(final TimedDistributedStorage timedDistributedStorage, final MeterRegistry meterRegistry) {
        this.timedDistributedStorage = timedDistributedStorage;

    }


    @Override
    public MessageResults read(List<String> types, long offset, String locationUuid) {
        return null;
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return null;
    }

    @Override
    public long getLatestOffsetMatching(List<String> types) {
        return 0;
    }

    @Override
    public void write(Message message) {

    }

    @Override
    public void write(OffsetEntity offset) {

    }

    @Override
    public void write(PipeState pipeState) {

    }

    @Override
    public void deleteAll() {

    }

    @Override
    public PipeState getPipeState() {
        return null;
    }
}
