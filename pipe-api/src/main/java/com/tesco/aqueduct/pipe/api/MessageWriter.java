package com.tesco.aqueduct.pipe.api;

public interface MessageWriter {

    /**
     * Put messages ordered by offset to a store.
     * Any missing offset should never arrive later.
     */
    default void write(Iterable<Message> messages) {
        messages.forEach(this::write);
    }

    void write(Message message);
    void write(OffsetEntity offset);
    void deleteAllMessages();
}
