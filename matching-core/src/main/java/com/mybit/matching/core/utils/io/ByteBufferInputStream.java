package com.mybit.matching.core.utils.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A byte buffer backed input inputStream
 */
public final class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int read() {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) {
        if (len == 0) {
            return 0;
        }
        if (!buffer.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, len);
        return len;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }
}
