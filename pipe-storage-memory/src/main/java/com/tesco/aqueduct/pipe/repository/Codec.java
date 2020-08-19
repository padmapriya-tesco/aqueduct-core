package com.tesco.aqueduct.pipe.repository;

public interface Codec {
    CodecType getType();
    byte[] encode(byte[] input);
    byte[] decode(byte[] input);
}
