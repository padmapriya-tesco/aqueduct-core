package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;

import java.util.OptionalLong;

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
}
