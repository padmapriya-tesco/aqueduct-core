package com.tesco.aqueduct.pipe.api;

import lombok.Data;

import java.util.List;
import java.util.OptionalLong;

@Data
public class MessageEntity {
    private final List<Message> messages;
    private final long retryAfterSeconds;
    private final OptionalLong globalLatestOffset;
}
