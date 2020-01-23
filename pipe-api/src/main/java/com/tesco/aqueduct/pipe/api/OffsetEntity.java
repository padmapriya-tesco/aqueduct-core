package com.tesco.aqueduct.pipe.api;

import lombok.Data;

import java.util.OptionalLong;

@Data
public class OffsetEntity {
    private final String name;
    private final OptionalLong value;
}
