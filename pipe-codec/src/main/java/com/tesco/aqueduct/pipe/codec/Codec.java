package com.tesco.aqueduct.pipe.codec;

public interface Codec {
    CodecType getType();
    byte[] encode(byte[] input);
    byte[] decode(byte[] input);
}
