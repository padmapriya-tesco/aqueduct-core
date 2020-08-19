package com.tesco.aqueduct.pipe.repository;

import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.tesco.aqueduct.pipe.api.JsonHelper;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * In memory repository of messages with optional patching on write and optional compression.
 *
 * Reasonable memory efficiency is achieved by storing messages with compressed byte arrays and decompressing them at read time.
 * Appending, finding and scanning efficiency is achieved by using a SkipList as a map, to map offsets to messages.
 * Patching is relatively good as we track previous offset in a hashmap.
 * This approach would also support deletions if needed.
 *
 * - can compress data in memory
 * - supports different codecs (only gzip at the moment)
 * - automatically decompress data on read
 * - smart in deciding should it encode or not per message (encoded might be bigger) with a ratio
 * - provide JSON patch where possible and sensible for data
 * - calculate patch on write, saving it only if it is smaller enough (with a ratio)
 * - efficient for writes - theoretically O(log N) practically constant for current use case
 * - efficient for finding indexes (or next available value if offset does not exist) - O(log N)
 * - efficient for full scans - once element is found it is O(1) to get the next one
 * - optimised for reading with patches - patches are generated during write time and stored compressed
 * - relatively good patching time - as we always know what the previous offset of a key was we can quickly find it and generate a patch
 *
 * Tested for 10gb of JSON on:
 *  - AdoptOpenJDK 14.0.2: -Xmx12g -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC
 *  - AdoptOpenJDK 1.8.0_252 - with defaults, heap
 *
 * Writes with patching are slow (10-30s for 100 000 messages of significant size), one can speed them up significantly by:
 * a) inserting small batches of messages (equal to number of CPUs you want to make busy) and generate patch in parallel
 * b) buffering puts, and patching them in parallel
 *
 * In both cases watch out for the same key in current group.
 * Another option is to generate patch on publish time and save both patch and data in central database.
 */
public class DiffingMessageRepository implements MessageRepository {

    private final NavigableMap<Long, BinaryMessageWithPatch> map = new ConcurrentSkipListMap<>();

    /**
     * Stores mapping between given offsets and it's last key so it is easy to apply a patch.
     *
     * It could also store reference do actual data (BinaryMessageWithPatch).
     * It would be faster (skip log n search for offset), but then care have to be taken
     * with compression and not to hold to too much data.
     */
    private final Map<String, Long> lastKeyOffset = new HashMap<>();

    /**
     * As the keys to offset mapping is not thread safe, put is not thread safe so it has a lock.
     */
    private final ReentrantLock lastKeyOffsetLock = new ReentrantLock();

    /**
     * There is high CPU cost for compression of data.
     * If compression does not give much, there is no point to waste CPU
     */
    private final double compressionRationThreshold = 0.8;

    /**
     * Undertanding patch on client side require significant IO to get previous version and CPU to apply it.
     * If patch does not give much, it is to costly to have it.
     */
    private final double patchRationThreshold = 0.8;

    /**
     * Should we try generating json patches.
     */
    private final boolean patch;

    private final Codec codec;

    public DiffingMessageRepository(boolean patch, Codec codec) {
        this.patch = patch;
        this.codec = codec;
    }

    protected BinaryMessageWithPatch encode(BinaryMessageWithPatch message) {
        if(codec == null) {
            return message;
        }

        // encode data
        byte[] data = message.getData();
        CodecType codecType = message.getDataCodecType();
        if(
            message.getData() != null &&
            message.getDataCodecType() == CodecType.NONE
        ) {
            byte[] encoded = codec.encode(message.getData());

            if(encoded.length < compressionRationThreshold * message.getData().length) {
                data = encoded;
                codecType = codec.getType();
            }
        }

        // encode patch
        BinaryPatch patch = message.getPatch();
        if(
            patch != null &&
            patch.getCodecType() == CodecType.NONE
        ) {
            byte[] encoded = codec.encode(patch.getPatch());

            if(encoded.length < compressionRationThreshold * patch.getPatch().length) {
                patch = message.getPatch()
                    .withPatch(encoded)
                    .withCodecType(codec.getType())
                ;
            }
        }

        // intern strings to save some extra space, this can reach gigabytes for tens of millions of messages
        return new BinaryMessageWithPatch(
            message.getType().intern(), // types repeat a lot, worth interning
            message.getKey(), // TODO: intern? have not measured difference for that field
            message.getContentType().intern(), // content types repeat a lot worth interning
            message.getOffset(),
            message.getCreated(),
            data,
            codecType,
            message.getSize(),
            patch
        );
    }

    protected BinaryMessageWithPatch decode(BinaryMessageWithPatch message) {
        if(codec == null) {
            return message;
        }

        // decode data
        if(message.getDataCodecType() == codec.getType()) {
            message = message
                .withData(codec.decode(message.getData()))
                .withDataCodecType(CodecType.NONE)
            ;
        }

        // decode patch
        if(message.getPatch() != null && message.getPatch().getCodecType() == codec.getType()) {
            message = message.withPatch(
                message.getPatch()
                    .withPatch(
                        codec.decode(message.getPatch().getPatch())
                    )
                    .withCodecType(CodecType.NONE)
            );
        }

        return message;
    }


    @Override
    public void put(@Nonnull BinaryMessageWithPatch message) {
        try {
            lastKeyOffsetLock.lock();
            Long previousOffset = lastKeyOffset.get(message.getKey());

            if (
                message.getPatch() == null &&
                previousOffset != null
            ) {
                BinaryMessageWithPatch previousMessage = get(previousOffset);
                if (previousMessage == null) {
                    throw new IllegalStateException("If we have previous offset we should have also previous message for message" + message.getKey());
                }

                message = generatePatch(previousMessage, message);
            }

            // easy to mess up, might move encoding to separate class
            map.put(message.getOffset(), encode(message));

            lastKeyOffset.put(message.getKey(),message.getOffset());
        } finally {
            lastKeyOffsetLock.unlock();
        }
    }

    @Override
    public BinaryMessageWithPatch get(@Nonnull Long offset) {
        return decode(map.get(offset));
    }

    @Override
    public Stream<? extends BinaryMessageWithPatch> streamFrom(@Nonnull Long offset) {
        return map.tailMap(offset).values().stream().map(this::decode);
    }

    protected BinaryMessageWithPatch generatePatch(BinaryMessageWithPatch encodedSource, BinaryMessageWithPatch potentiallyEncodedTarget) {
        if(
            patch &&
            "application/json".equals(encodedSource.getContentType()) &&
            "application/json".equals(potentiallyEncodedTarget.getContentType()) &&
            !"runscope-test-type".equals(encodedSource.getType()) && //FIXME: why oh why runscope send invalid json as application/json!!!
            encodedSource.getData() != null &&
            potentiallyEncodedTarget.getData() != null
        ) {
            byte[] sourceData = decode(encodedSource).getData();
            BinaryMessageWithPatch plainTarget = decode(potentiallyEncodedTarget);
            byte[] targetData = plainTarget.getData();

            try {
                //
                JsonPatch jsonPatch = JsonDiff.asJsonPatch(
                    JsonHelper.MAPPER.readTree(sourceData),
                    JsonHelper.MAPPER.readTree(targetData)
                );
                byte[] patchBytes = JsonHelper.MAPPER.writeValueAsBytes(jsonPatch);

                if(patchBytes.length < patchRationThreshold * targetData.length) {
                    // don't need to encode as this happens during "put"
                    return plainTarget.withPatch(
                        new BinaryPatch(
                            encodedSource.getOffset(),
                            patchBytes,
                            CodecType.NONE,
                            patchBytes.length
                        )
                    );
                }
            } catch (IOException e) {
                // TODO: log failure
                //e.printStackTrace();
                System.out.println("PANIC! " + e.getMessage());
                System.out.println(new String(sourceData));
                System.out.println(new String(targetData));
            }
        }

        // do not patch
        return potentiallyEncodedTarget;
    }
}
