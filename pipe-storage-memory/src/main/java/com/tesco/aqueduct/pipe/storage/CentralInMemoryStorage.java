package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.OffsetEntity;
import com.tesco.aqueduct.pipe.api.OffsetName;
import com.tesco.aqueduct.pipe.api.PipeState;

import java.util.OptionalLong;

public class CentralInMemoryStorage extends InMemoryStorage {
    public CentralInMemoryStorage(int limit, long retryAfter) {
        super(limit, retryAfter);
    }

    @Override
    protected OptionalLong getLatestGlobalOffset() {
        return messages.stream().mapToLong(Message::getOffset).max();
    }

    @Override
    public void write(OffsetEntity offset) { }

    @Override
    public void write(PipeState pipeState) { }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return messages.stream().mapToLong(Message::getOffset).max();
    }
}
