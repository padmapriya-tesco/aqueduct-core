package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.api.MessageWriter;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import lombok.val;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

public class InMemoryStorage implements MessageReader, MessageWriter {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(InMemoryStorage.class));

    private final long retryAfter;

    // old fashion read-write lock - fast reads, blocking writes
    final private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    final private List<Message> messages = new ArrayList<>();
    final private int limit;

    public InMemoryStorage(final int limit, final long retryAfter) {
        this.limit = limit;
        this.retryAfter = retryAfter;
    }

    /**
     * Complexity: O(log(n)+limit)
     */
    @Override
    public MessageResults read(final List<String> types, final long offset) {
        val lock = rwl.readLock();

        LOG.withTypes(types).debug("in memory storage", "reading with types");

        try {
            lock.lock();

            int index = findIndex(offset);

            if (index >= 0) {
                // found
                return new MessageResults(readFrom(types,index), 0);
            } else {
                // determine if at the head of the queue to return retry after
                long retry = getRetry(offset);

                // not found
                return new MessageResults(readFrom(types,-index-1), retry);
            }
        } finally {
            lock.unlock();
        }
    }

    private long getRetry(final long offset) {
        return messages.isEmpty() || offset >= messages.get(messages.size()-1).getOffset() ? retryAfter : 0;
    }

    @Override
    public long getLatestOffsetMatching(final List<String> types) {
        for (int i = messages.size()-1 ; i >= 0 ; i--) {
            Message message = messages.get(i);

            if (messageMatchTypes(message, types)) {
                return message.getOffset();
            }
        }
        return 0;
    }

    private boolean messageMatchTypes(final Message message, final List<String> types) {
        return types == null || types.isEmpty() || types.contains(message.getType());
    }

    /**
     * @return If found returns non negative index of element.
     *         If not found returns negative index = (-expectedPosition) - 1.
     *         Where expected position is the index where the element would finish after inserting.
     */
    private int findIndex(final long offset) {
        return Collections.binarySearch(
            messages,
            new Message(null, null, null, offset, null, null),
            comparingLong(Message::getOffset)
        );
    }

    /**
     * Complexity: avg O(limit), worst case O(size), where limit is number of elements taken
     */
    private List<Message> readFrom(final List<String> types, final int index) {
        return messages
            .stream()
            .skip(index)
            .filter(m -> messageMatchTypes(m, types))
            .limit(limit)
            .collect(Collectors.toList())
        ;
    }

    /**
     * Complexity (assuming O(1) for map operations and array insert):
     * - with no compaction - O(1)
     * - with compaction - O(log(n))
     *
     * In practice remove(x) can take a O(n) and hashmap is O(1) only on average
     */
    @Override
    public void write(Message message) {
        val lock = rwl.writeLock();

        LOG.withMessage(message).info("in memory storage", "writing message");

        try {
            lock.lock();
            long lastOffset = 0;

            if (!messages.isEmpty()) {
                lastOffset = messages.get(messages.size() - 1).getOffset();

                 if (message.getOffset() != null && message.getOffset() <= lastOffset) {
                    throw new IllegalStateException("Messages can be only written in ascending order of offsets");
                }
            }

            if (message.getOffset() == null) {
                message = message.withOffset(lastOffset + 1);
            }

            messages.add(message);
        }finally {
            lock.unlock();
        }
    }

    public void clear() {
        val lock = rwl.writeLock();

        try {
            lock.lock();
            messages.clear();
        } finally {
            lock.unlock();
        }
    }
}
