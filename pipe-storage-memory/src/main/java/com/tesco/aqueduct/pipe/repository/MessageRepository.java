package com.tesco.aqueduct.pipe.repository;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

public interface MessageRepository {
    /**
     * Put message on it's offset.
     * Message should have offset assigned already and for consistency of consumers put should be called
     * with monotonically increasing offset of the message.
     *
     * Implementation is not required to test that.
     */
    void put(@Nonnull BinaryMessageWithPatch message);

    /**
     * Method might return null as message might not exist.
     */
    @Nullable BinaryMessageWithPatch get(@Nonnull Long offset);

    /**
     * Starts streaming messages since given offset, inclusive.
     * Implementation is allowed, but not required to limit how many values are emmited on single call.
     * Hence reaching end of the stream does not mean we have no more elements.
     */
    Stream<? extends BinaryMessageWithPatch> streamFrom(@Nonnull Long offset);
}
