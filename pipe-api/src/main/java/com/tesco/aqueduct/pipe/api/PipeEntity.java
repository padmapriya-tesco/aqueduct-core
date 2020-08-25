package com.tesco.aqueduct.pipe.api;

import lombok.Data;

import java.util.List;

@Data
public class PipeEntity {
    private final List<Message> messages;
    private final List<OffsetEntity> offsets;
}
