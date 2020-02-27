package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

public class MetricizedDistributedStorage implements DistributedStorage {

    private final Map<OffsetName, AtomicLong> atomicOffsets = new HashMap<>();
    private final TimedDistributedStorage timedDistributedStorage;

    public MetricizedDistributedStorage(final TimedDistributedStorage timedDistributedStorage, final MeterRegistry meterRegistry) {
        this.timedDistributedStorage = timedDistributedStorage;

        atomicOffsets.put(OffsetName.GLOBAL_LATEST_OFFSET, meterRegistry.gauge("pipe.offset.globalLatestOffset", new AtomicLong(0)));
        atomicOffsets.put(OffsetName.LOCAL_LATEST_OFFSET, meterRegistry.gauge("pipe.offset.localLatestOffset", new AtomicLong(0)));
        atomicOffsets.put(OffsetName.PIPE_OFFSET, meterRegistry.gauge("pipe.offset.pipeOffset", new AtomicLong(0)));
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
        timedDistributedStorage.write(message);
    }

    @Override
    public void write(OffsetEntity offset) {
        AtomicLong atomicOffset = atomicOffsets.get(offset.getName());
        atomicOffset.set(offset.getValue().getAsLong());
        timedDistributedStorage.write(offset);
    }

    @Override
    public void write(PipeState pipeState) { }

    @Override
    public void deleteAll() { }

    @Override
    public PipeState getPipeState() {
        return null;
    }
}
