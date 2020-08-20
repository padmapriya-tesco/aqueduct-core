package com.tesco.aqueduct.pipe.http.codec;

public interface Codec {
    CodecType getType();
    byte[] encode(byte[] input);
    byte[] decode(byte[] input);
}
