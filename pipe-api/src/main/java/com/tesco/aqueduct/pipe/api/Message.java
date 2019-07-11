package com.tesco.aqueduct.pipe.api;

import lombok.Data;
import lombok.experimental.Wither;

import java.time.ZonedDateTime;

@Data
@Wither
public class Message {
    private final String type;
    private final String key;
    private final String contentType;
    private final Long offset;

    private final ZonedDateTime created;

    private final String data;

    private static final int MAX_OFFSET_LENGTH = 19;
    private static final int MAX_DATE_LENGTH = 64;
    private static final int EXTRA_ENCODING_CHARACTERS = 6;

    public static final int MAX_OVERHEAD_SIZE = MAX_OFFSET_LENGTH + MAX_DATE_LENGTH + EXTRA_ENCODING_CHARACTERS;

    public Message(
        final String type,
        final String key,
        final String contentType,
        final Long offset,
        final ZonedDateTime created,
        final String data
    ) {
        this.offset = offset;
        this.key = key;
        this.type = type;
        this.contentType = contentType;
        this.created = created;
        this.data = data;
    }
}
