package kafka.message;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {
    public final ByteBuffer buffer;

    public ByteBufferBackedInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        return buffer.hasRemaining() ? (buffer.get() & 0xFF) : -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
        if (buffer.hasRemaining()) {
            // Read only what's left
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        } else {
            return -1;
        }
    }
}
