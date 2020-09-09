package com.tesco.aqueduct.pipe.codec;

import lombok.Getter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

import static com.tesco.aqueduct.pipe.api.HttpHeaders.X_CONTENT_ENCODING;
import static io.micronaut.http.HttpHeaders.CONTENT_ENCODING;

public class CodecHelper {

    @Inject BrotliCodec brotliCodec;
    @Inject GzipCodec gzipCodec;

    public EncodedResponse encodeResponse(String acceptEncodingHeader, byte[] responseBytes) {
        Map<CharSequence, CharSequence> headers = new HashMap<>();
        byte[] responseBody = responseBytes;
        if (acceptEncodingHeader.contains("br")) {
            responseBody = brotliCodec.encode(responseBytes);
            headers.put(X_CONTENT_ENCODING, brotliCodec.getHeaderType());
        } else if (acceptEncodingHeader.contains("gzip")) {
            responseBody = gzipCodec.encode(responseBytes);
            headers.put(X_CONTENT_ENCODING, gzipCodec.getHeaderType());
            headers.put(CONTENT_ENCODING, gzipCodec.getHeaderType());
        }

        return new EncodedResponse(responseBody, headers);
    }

    @Getter
    public class EncodedResponse {
        byte[] encodedBody;
        Map<CharSequence, CharSequence> headers;

        public EncodedResponse(byte[] encodedBody, Map<CharSequence, CharSequence> headers) {
            this.encodedBody = encodedBody;
            this.headers = headers;
        }
    }
}
