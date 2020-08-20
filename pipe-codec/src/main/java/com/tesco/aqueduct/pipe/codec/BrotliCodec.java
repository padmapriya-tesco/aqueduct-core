package com.tesco.aqueduct.pipe.codec;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.dec.BrotliInputStream;
import com.nixxcode.jvmbrotli.enc.BrotliOutputStream;
import com.nixxcode.jvmbrotli.enc.Encoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BrotliCodec implements Codec {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(BrotliCodec.class));

    public BrotliCodec() {
        final boolean isBrotliAvailable = BrotliLoader.isBrotliAvailable();
        LOG.info("Codec", "Load Brotli: " + isBrotliAvailable);
    }

    @Override
    public byte[] encode(byte[] input) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BrotliOutputStream brotliOutputStream =
                 new BrotliOutputStream(outputStream, new Encoder.Parameters().setQuality(4))) {
            brotliOutputStream.write(input);
        } catch (IOException ioException) {
            LOG.error("Codec", "Error encoding response.", ioException);
            throw new PipeCodecException("Error encoding response.", ioException);
        }
        final byte[] encodedBytes = outputStream.toByteArray();
        // TODO probably not required for production
        LOG.info("Codec", "Compression ratio: " + ((double)input.length)/((double)encodedBytes.length));
        return encodedBytes;
    }

    @Override
    public byte[] decode(byte[] input) {
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final BrotliInputStream brotliInputStream = new BrotliInputStream(new ByteArrayInputStream(input))) {

            int decodedByte=brotliInputStream.read();
            while (decodedByte != -1) {
                byteArrayOutputStream.write(decodedByte);
                decodedByte = brotliInputStream.read();
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException ioException) {
            LOG.error("Codec", "Error decoding bytes.", ioException);
            throw new PipeCodecException("Error decoding bytes.", ioException);
        }
    }

    @Override
    public CodecType getType() {
        return CodecType.BROTLI;
    }
}

