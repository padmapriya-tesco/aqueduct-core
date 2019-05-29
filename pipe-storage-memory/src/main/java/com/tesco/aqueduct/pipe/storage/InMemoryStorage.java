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

    public InMemoryStorage(int limit, long retryAfter) {
        this.limit = limit;
        this.retryAfter = retryAfter;
    }

    /**
     * Complexity: O(log(n)+limit)
     */
    @Override
    public MessageResults read(Map<String, List<String>> tags, long offset) {
        val lock = rwl.readLock();

        LOG.withTags(tags).debug("in memory storage", "reading with tags");

        try {
            lock.lock();

            int index = findIndex(offset);

            if (index >= 0) {
                // found
                return new MessageResults(readFrom(tags,index), 0);
            } else {
                // determine if at the head of the queue to return retry after
                long retry = getRetry(offset);

                // not found
                return new MessageResults(readFrom(tags,-index-1), retry);
            }
        } finally {
            lock.unlock();
        }
    }

    private long getRetry(long offset) {
        return messages.isEmpty() || offset >= messages.get(messages.size()-1).getOffset() ? retryAfter : 0;
    }

    @Override
    public long getLatestOffsetMatching(Map<String, List<String>> tags) {
        for (int i = messages.size()-1 ; i >= 0 ; i--) {
            Message message = messages.get(i);
            if (tagsMatch(message, tags)) {
                return message.getOffset();
            }
        }
        return 0;
    }

    /**
     * Returns true if for all keys in filters, there is at least one value that exist in the tags.
     */
    boolean tagsMatch(Message m, Map<String, List<String>> tagFilters) {
        if (tagFilters == null) {
            return true;
        }

        Map<String, List<String>> messageTags = m.getTags();

        if (messageTags == null && !tagFilters.isEmpty()) {
            return false;
        }

        if (tagFilters.containsKey("type") && !tagFilters.get("type").contains(m.getType())) {
            return false;
        }

        return tagFilters.entrySet().stream()
            .filter(filterTag -> !"type".equals(filterTag.getKey()))
            .allMatch(filterTag ->
                filterTag.getValue().stream().anyMatch(singleTagValue ->
                    messageTags.getOrDefault(filterTag.getKey(), Collections.emptyList()).contains(singleTagValue)
                )
        );
    }

    /**
     * @return If found returns non negative index of element.
     *         If not found returns negative index = (-expectedPosition) - 1.
     *         Where expected position is the index where the element would finish after inserting.
     */
    private int findIndex(long offset) {
        return Collections.binarySearch(
            messages,
            new Message(null, null, null, offset, null, null, null),
            comparingLong(Message::getOffset)
        );
    }

    /**
     * Complexity: avg O(limit), worst case O(size), where limit is number of elements taken
     */
    private List<Message> readFrom(Map<String, List<String>> tags, int index) {
        return messages
            .stream()
            .skip(index)
            .filter(m -> tagsMatch(m, tags))
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
