package com.tesco.aqueduct.pipe.api;

import lombok.Data;

import java.util.List;

@Data
public class MessageResults {
    private final List<Message> messages;
    private final long retryAfterSeconds;
}
