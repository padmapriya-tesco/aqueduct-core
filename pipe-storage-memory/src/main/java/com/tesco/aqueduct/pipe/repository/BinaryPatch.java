package com.tesco.aqueduct.pipe.repository;


import lombok.Value;
import lombok.With;

@Value
public class BinaryPatch {
    long source;
    @With byte[] patch;
    @With CodecType codecType;

    /**
     * Size of the patch without compression
     */
    int size;

    // Currently we only support json patch for content type json but other are possible - plain patch, binary delta, etc.
    // PatchFormat format
}
