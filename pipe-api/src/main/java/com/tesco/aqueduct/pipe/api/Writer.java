package com.tesco.aqueduct.pipe.api;

public interface Writer {

    /**
     * Put messages ordered by offset to a store.
     * Any missing offset should never arrive later.
     */
    default void write(Iterable<Message> messages) {
        messages.forEach(this::write);
    }

    void write(Message message);
    void write(OffsetEntity offset);
    void write(PipeState pipeState);
    void deleteAll();
}
