package com.tesco.aqueduct.pipe.http.codec;

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

        Encoder.Parameters params = new Encoder.Parameters().setQuality(4);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        long startTimeNano = System.nanoTime();
        try (BrotliOutputStream brotliOutputStream = new BrotliOutputStream(outputStream, params)) {
            brotliOutputStream.write(input);

        } catch (IOException ioException) {
            LOG.error("Codec", "Error encoding response.", ioException);
            throw new RuntimeException("Error encoding response.", ioException); // TODO bit more specific
        }
        long endTimeNano = System.nanoTime();

        final byte[] encodedBytes = outputStream.toByteArray();

        // TODO remove them if not required
        LOG.info("Codec", "Time taken to encode in nanoseconds: " + (endTimeNano - startTimeNano));
        LOG.info("Codec", "UnCompressed size: " + input.length);
        LOG.info("Codec", "Compressed size: " + encodedBytes.length);
        LOG.info("Codec", "Compression ratio: " + ((double)input.length)/((double)encodedBytes.length));

        return encodedBytes;
    }

    @Override
    public byte[] decode(byte[] input) {

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (BrotliInputStream brotliInputStream = new BrotliInputStream(new ByteArrayInputStream(input))) {
            int decodedByte=brotliInputStream.read();

            while (decodedByte != -1) {
                byteArrayOutputStream.write(decodedByte);
                decodedByte = brotliInputStream.read();
            }

            return byteArrayOutputStream.toByteArray();

        } catch (IOException ioException) {
            LOG.error("Codec", "Error decoding bytes.", ioException);
            throw new RuntimeException("Error decoding bytes.", ioException); // TODO bit more specific
        }
    }

    @Override
    public CodecType getType() {
        return CodecType.BROTLI;
    }

}

