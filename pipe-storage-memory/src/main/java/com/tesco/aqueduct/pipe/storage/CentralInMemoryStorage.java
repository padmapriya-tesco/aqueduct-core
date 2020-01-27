package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.OffsetEntity;

import java.util.OptionalLong;

public class CentralInMemoryStorage extends InMemoryStorage {
    public CentralInMemoryStorage(int limit, long retryAfter) {
        super(limit, retryAfter);
    }

    @Override
    protected OptionalLong getGlobalOffset() {
        return messages.stream().mapToLong(Message::getOffset).max();
    }

    @Override
    public void write(OffsetEntity offset) { }

}
